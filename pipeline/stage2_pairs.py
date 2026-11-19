"""Stage 2. Find pairs inside HEALPix cells. Shuffle by cell index.

Input:  stage1_stars.parquet
Output: stage2_pairs.parquet - pairs (source_id_1 < source_id_2) with θ, s, Δμ
No combiner is possible: the pair search needs the whole cell at once.
"""
import functools

from pyspark.sql import functions as F

from pipeline import config, geometry, metrics


def find_pairs(stars, pair_function=geometry.pairs_in_cell, all_neighbours=False):
    """Build the lazy pair search plan shared by stages 2 and 5.

    Every star is copied into neighbouring cells, rows are grouped by cell and
    pair_function searches for pairs inside each group. Returns (copies, pairs):
    copies are used for measurements, pairs is the result. With all_neighbours=True
    stars are copied into all 8 neighbours and dropDuplicates removes pairs found twice.
    This variant is for stage 2 only: on stage 5 it would merge distinct pairs (a, b') and (b, a').
    """
    if all_neighbours:
        copies = stars.mapInPandas(functools.partial(geometry.add_copies, all_neighbours=True),
                                   schema=geometry.COPY_SCHEMA)
        pairs = copies.groupBy("cell").applyInPandas(pair_function, schema=geometry.PAIR_SCHEMA)
        return copies, pairs.dropDuplicates(["source_id_1", "source_id_2"])

    copies = stars.mapInPandas(geometry.add_copies, schema=geometry.COPY_SCHEMA)
    pairs = copies.groupBy("cell").applyInPandas(pair_function, schema=geometry.PAIR_SCHEMA)
    return copies, pairs


def comparisons(copies):
    """Count how many star pairs stage 2 compares.

    A cell with h home stars and g copies compares h·(h−1)/2 home-home pairs
    and h·g home-copy pairs. Runs a separate job.
    """
    per_cell = copies.groupBy("cell").agg(
        F.sum(F.col("is_home").cast("long")).alias("h"),
        F.sum((~F.col("is_home")).cast("long")).alias("g"),
    )
    return per_cell.agg(F.sum(F.col("h") * (F.col("h") - 1) / 2 + F.col("h") * F.col("g"))).first()[0]


def run(spark, stars, measure=True, all_neighbours=False):
    """Find pairs among the stage 1 stars and write them to config.PAIRS.

    With measure=True, extra jobs count the rows going into the shuffle,
    the number of comparisons and the key distribution. They are turned off
    for timing runs. Returns a DataFrame of the written pairs.
    """
    metrics.record("stage2", variant="all_neighbours" if all_neighbours else "lower_neighbours")
    copies, pairs = find_pairs(stars, all_neighbours=all_neighbours)
    metrics.save_plan(pairs, "stage2")

    with metrics.job("write pairs"):
        pairs.write.mode("overwrite").parquet(str(config.PAIRS))

    out = spark.read.parquet(str(config.PAIRS))
    with metrics.job("count pairs"):
        metrics.record("stage2", pairs_out=out.count())
    if measure:
        with metrics.job("measure: rows into shuffle"):
            metrics.record("stage2", rows_into_shuffle=copies.count())
        with metrics.job("measure: comparisons"):
            metrics.record("stage2", comparisons=comparisons(copies))
        with metrics.job("measure: key distribution"):
            metrics.key_distribution(copies, "cell", "stage2_skew")
    return out
