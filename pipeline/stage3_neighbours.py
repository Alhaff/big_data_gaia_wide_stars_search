"""Stage 3. Count companions of every star and drop clusters. Shuffle by source_id.

Input:  stage2_pairs.parquet
Output: stage3_pairs_clean.parquet - pairs where both stars have at most MAX_COMPANIONS companions
A combiner is possible: count is partially computed before the shuffle.
"""
from pyspark.sql import functions as F

from pipeline import config, metrics


def companions(pairs):
    """Count how many pairs each star belongs to.

    Every pair gives two rows, one per star, and the rows are grouped
    by source_id. Returns a lazy DataFrame (source_id, n).
    """
    ends = pairs.select(F.col("source_id_1").alias("source_id")).union(
        pairs.select(F.col("source_id_2").alias("source_id")))
    return ends.groupBy("source_id").count().withColumnRenamed("count", "n")


def drop_crowded(pairs, counts):
    """Remove pairs where either star has more than MAX_COMPANIONS companions.

    Such stars most likely belong to a cluster or a moving group rather than
    a binary. They are removed with two anti joins. Returns a lazy DataFrame of pairs.
    """
    crowded = counts.filter(F.col("n") > config.MAX_COMPANIONS).select("source_id")
    return (pairs
            .join(crowded.withColumnRenamed("source_id", "source_id_1"), "source_id_1", "left_anti")
            .join(crowded.withColumnRenamed("source_id", "source_id_2"), "source_id_2", "left_anti"))


def run(spark, pairs):
    """Clean the stage 2 pairs of clusters and write the result to config.PAIRS_CLEAN.

    A separate job computes the companion count histogram for the metrics.
    Returns a DataFrame of the written pairs.
    """
    counts = companions(pairs)
    clean = drop_crowded(pairs, counts)
    metrics.save_plan(clean, "stage3")

    with metrics.job("write clean pairs"):
        clean.write.mode("overwrite").parquet(str(config.PAIRS_CLEAN))

    out = spark.read.parquet(str(config.PAIRS_CLEAN))
    with metrics.job("companions histogram"):
        hist = {r["n"]: r["count"] for r in counts.groupBy("n").count().orderBy("n").collect()}
    with metrics.job("count clean pairs"):
        metrics.record("stage3", pairs_out=out.count(), companions_histogram=hist)
    return out
