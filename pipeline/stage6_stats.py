"""Stage 6. Real pairs and chance alignments per projected separation bin. Shuffle by bin.

bin = floor(log10(s) · 2): two bins per decade (100-316 AU, 316-1000 AU, ...).
Pairs with θ < 0.5″ are not counted, every chance pair has weight CHANCE_WEIGHT.
"""
from pyspark.sql import functions as F

from pipeline import config, metrics


def per_bin(pairs, name):
    """Count pairs with θ ≥ THETA_MIN_ARCSEC in every separation bin.

    Returns a lazy DataFrame (bin, name), where name is the count column.
    """
    return (pairs.filter(F.col("theta_arcsec") >= config.THETA_MIN_ARCSEC)
            .withColumn("bin", F.floor(F.log10("s_au") * 2))
            .groupBy("bin").count().withColumnRenamed("count", name))


def run(spark, pairs_clean, pairs_chance):
    """Build the "real pairs vs chance alignments" table and write it to config.STATS.

    For every bin it computes the bounds in AU, the weighted number of chance pairs
    and the chance alignment fraction among found pairs. Returns a pandas table.
    """
    real = per_bin(pairs_clean, "real")
    chance = per_bin(pairs_chance, "chance")
    stats = (real.join(chance, "bin", "full_outer").fillna(0)
             .withColumn("chance", F.col("chance") * config.CHANCE_WEIGHT)
             .withColumn("s_from_au", F.pow(10, F.col("bin") / 2))        # bin bounds: 10^(bin/2)
             .withColumn("s_to_au", F.pow(10, (F.col("bin") + 1) / 2))
             .withColumn("chance_fraction", F.col("chance") / F.col("real"))
             .orderBy("bin"))

    metrics.save_plan(stats, "stage6")
    with metrics.job("collect stats"):
        table = stats.toPandas()
    config.OUTPUT.mkdir(parents=True, exist_ok=True)
    table.to_csv(config.STATS, index=False)
    print(table.to_string(index=False), flush=True)
    return table
