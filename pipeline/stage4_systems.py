"""Stage 4. Star systems as connected components of the pair graph, by label propagation.

Input:  stage3_pairs_clean.parquet (graph edges)
Output: stage4_systems.parquet - (source_id, label), label = the smallest source_id of the system
Shuffle on every iteration (groupBy(source_id).min) and at the end (groupBy(label)).
"""
from pyspark.sql import functions as F

from pipeline import config, metrics


def run(spark, pairs):
    """Merge pairs that share stars into systems and write the labels to config.SYSTEMS.

    Initially every star is its own system labelled with its source_id. On each
    iteration a star takes the smallest label among its own and its neighbours',
    until no label changes. System sizes are recorded in the metrics.
    Returns a DataFrame (source_id, label).
    """
    edges = pairs.select(F.col("source_id_1").alias("src"), F.col("source_id_2").alias("dst")).union(
        pairs.select(F.col("source_id_2").alias("src"), F.col("source_id_1").alias("dst")))
    labels = edges.select(F.col("src").alias("source_id")).distinct().withColumn("label", F.col("source_id"))

    for iteration in range(1, config.MAX_ITERATIONS + 1):
        from_neighbours = (edges.join(labels, edges.src == labels.source_id)
                           .select(F.col("dst").alias("source_id"), "label"))
        new_labels = labels.union(from_neighbours).groupBy("source_id").agg(F.min("label").alias("label"))

        if iteration == 1:
            metrics.save_plan(new_labels, "stage4_iteration")

        # localCheckpoint cuts the lineage, otherwise each iteration's plan contains all previous ones
        with metrics.job(f"iteration {iteration}: new labels"):
            new_labels = new_labels.localCheckpoint()

        with metrics.job(f"iteration {iteration}: count changed"):
            changed = (new_labels.alias("n").join(labels.alias("o"), "source_id")
                       .filter(F.col("n.label") != F.col("o.label")).count())

        print(f"  ітерація {iteration}: змінилося міток {changed}", flush=True)
        labels = new_labels
        if changed == 0:
            break
    metrics.record("stage4", iterations=iteration)

    with metrics.job("write systems"):
        labels.write.mode("overwrite").parquet(str(config.SYSTEMS))

    per_system = labels.groupBy("label").count()
    with metrics.job("system sizes"):
        sizes = {r["count"]: r["systems"]
                 for r in per_system.groupBy("count").agg(F.count("*").alias("systems")).collect()}
    metrics.record("stage4", system_sizes=sizes)
    return spark.read.parquet(str(config.SYSTEMS))
