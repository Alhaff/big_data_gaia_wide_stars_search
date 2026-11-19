"""Stage 5. Chance alignments: the stage 2-3 pair search on "original + copy shifted by 0.5°".

Original-copy pairs cannot be real, because these stars are physically far apart.
Their count in a separation bin estimates how many chance alignments the real pairs
in the same bin contain. Shuffles by cell and by source_id.
"""
from pipeline import config, geometry, metrics, stage2_pairs, stage3_neighbours


def run(spark, stars):
    """Find original-shifted copy pairs and write them to config.PAIRS_SHIFTED.

    The catalog copy is shifted in dec, unioned with the original, and the same steps
    as on stages 2 and 3 are run, counting only pairs across the two sets.
    Returns a DataFrame of the written chance pairs.
    """
    shifted = stars.mapInPandas(geometry.shift_catalog, schema=geometry.STAR_SCHEMA)
    both = stars.unionByName(shifted)
    _, pairs = stage2_pairs.find_pairs(both, pair_function=geometry.chance_pairs_in_cell)

    # pairs is read three times and AQE does not reuse applyInPandas above the shuffle,
    # so the pairs are computed once and kept locally
    metrics.save_plan(pairs, "stage5_pairs")
    with metrics.job("find chance pairs"):
        pairs = pairs.localCheckpoint()

    chance = stage3_neighbours.drop_crowded(pairs, stage3_neighbours.companions(pairs))
    metrics.save_plan(chance, "stage5")

    with metrics.job("write chance pairs"):
        chance.write.mode("overwrite").parquet(str(config.PAIRS_SHIFTED))

    out = spark.read.parquet(str(config.PAIRS_SHIFTED))
    with metrics.job("count chance pairs"):
        metrics.record("stage5", chance_pairs_out=out.count())
    return out
