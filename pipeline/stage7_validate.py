"""Stage 7. Validation against the El-Badry, Rix & Heintz (2021) catalog. Join on the source_id pair.

Catalog: https://zenodo.org/records/4435257, file all_columns_catalog.fits.gz (CC BY 4.0).
Spark cannot read FITS, so on the first run the needed columns are converted to Parquet.

Their pairs are restricted to our sky region (both stars in the input files) and to our
limits: 1 ≤ ϖ ≤ 10 mas for both stars and s < 50 000 AU. The star filter and pair criteria
are the same, so every one of their pairs within these limits must already be found on stage 2.

Metrics: completeness (share of their pairs found by us) for stages 2 and 3, purity of stage 3
(share of our pairs present in their catalog), the same for high-confidence pairs (R < 0.1) and per s bin.
"""
import functools
import json

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from pipeline import config, metrics

# column names in their catalog -> ours
CATALOG_COLUMNS = {
    "source_id1": "eb_id_1",
    "source_id2": "eb_id_2",
    "parallax1": "eb_parallax_1",
    "parallax2": "eb_parallax_2",
    "pairdistance": "eb_theta_deg",
    "sep_AU": "eb_s_au",
    "R_chance_align": "eb_r",
    "binary_type": "eb_type",
}


def convert_catalog():
    """Convert the catalog from FITS to Parquet (config.ELBADRY), CATALOG_COLUMNS only.

    Runs without Spark, using astropy. The .fits.gz file is first unpacked to a temporary
    file so that astropy reads it via memmap and loads only the needed columns into
    memory instead of the whole catalog. The temporary file is removed even on error.
    """
    import gzip
    import shutil

    from astropy.io import fits

    tmp = config.OUTPUT / "elbadry_tmp.fits"
    config.OUTPUT.mkdir(parents=True, exist_ok=True)
    print(f"  розпаковую {config.ELBADRY_FITS.name} -> {tmp.name}", flush=True)
    with gzip.open(config.ELBADRY_FITS, "rb") as src, open(tmp, "wb") as dst:
        shutil.copyfileobj(src, dst, 1 << 24)
    try:
        _fits_to_parquet(fits, tmp)
    finally:
        tmp.unlink()


def _fits_to_parquet(fits, path):
    print(f"  читаю колонки -> {config.ELBADRY.name}", flush=True)
    with fits.open(path, memmap=True) as hdul:
        data = hdul[1].data
        names = {n.lower(): n for n in hdul[1].columns.names}
        missing = [c for c in CATALOG_COLUMNS if c.lower() not in names]
        if missing:
            raise KeyError(f"у каталозі немає колонок {missing}; є: {sorted(names.values())}")
        out = {}
        for theirs, ours in CATALOG_COLUMNS.items():
            col = np.asarray(data[names[theirs.lower()]])
            if col.dtype.kind in "SU":
                col = col.astype(str)
            else:
                col = col.astype(col.dtype.newbyteorder("="))  # FITS is big-endian, Arrow needs native order
            out[ours] = col
    pdf = pd.DataFrame(out)
    pdf.to_parquet(config.ELBADRY, index=False)
    print(f"  {len(pdf):,} пар записано".replace(",", " "), flush=True)


def in_our_region(id_col, ranges):
    """Return a Spark condition: the star's level-8 HEALPix pixel lies in one of the ranges.

    The pixel is taken directly from the high bits of source_id, so no coordinates are needed.
    """
    pix = F.shiftright(id_col, config.CELL_SHIFT)
    return functools.reduce(lambda a, b: a | b, [pix.between(lo, hi) for lo, hi in ranges])


def ordered(a, b):
    """Return two columns (id_a, id_b): the smaller and the larger source_id of a pair, as on stage 2."""
    return F.least(a, b).alias("id_a"), F.greatest(a, b).alias("id_b")


def run(spark, pairs, pairs_clean):
    """Compare the stage 2 and 3 pairs with the El-Badry catalog and record completeness and purity.

    Totals go to metrics.json, the per-bin table to config.VALIDATION and up to 20 examples
    of missed pairs to stage7_missed_pairs.csv. Returns a dict of the totals.
    """
    if not config.ELBADRY.exists():
        convert_catalog()
    manifest = json.loads(config.MANIFEST.read_text())
    ranges = [(f["pix_first"], f["pix_last"]) for f in manifest["files"]]

    catalog = spark.read.parquet(str(config.ELBADRY))
    theirs = (catalog
              .filter(in_our_region(F.col("eb_id_1"), ranges) & in_our_region(F.col("eb_id_2"), ranges))
              .filter(F.col("eb_parallax_1").between(config.PARALLAX_MIN, config.PARALLAX_MAX))
              .filter(F.col("eb_parallax_2").between(config.PARALLAX_MIN, config.PARALLAX_MAX))
              .filter(F.col("eb_s_au") < config.S_MAX_AU)
              .select(*ordered(F.col("eb_id_1"), F.col("eb_id_2")), "eb_s_au", "eb_r", "eb_type",
                      F.lit(True).alias("in_theirs")))

    ours2 = pairs.select(F.col("source_id_1").alias("id_a"), F.col("source_id_2").alias("id_b"),
                         F.col("s_au"), F.lit(True).alias("in_stage2"))
    ours3 = pairs_clean.select(F.col("source_id_1").alias("id_a"), F.col("source_id_2").alias("id_b"),
                               F.lit(True).alias("in_stage3"))

    # full outer: both missed and extra pairs are needed
    joined = (theirs.join(ours2, ["id_a", "id_b"], "full_outer")
              .join(ours3, ["id_a", "id_b"], "left_outer")
              .fillna(False, ["in_theirs", "in_stage2", "in_stage3"])
              .withColumn("s", F.coalesce("eb_s_au", "s_au"))
              .withColumn("bin", F.floor(F.log10("s") * 2))
              .withColumn("high_conf", F.col("in_theirs") & (F.col("eb_r") < config.R_HIGH_CONFIDENCE)))
    metrics.save_plan(joined, "stage7")

    def n(cond):
        return F.sum(F.when(cond, 1).otherwise(0))

    counts = [
        n(F.col("in_theirs")).alias("theirs"),
        n(F.col("high_conf")).alias("theirs_r01"),
        n(F.col("in_stage2")).alias("ours_stage2"),
        n(F.col("in_stage3")).alias("ours_stage3"),
        n(F.col("in_theirs") & F.col("in_stage2")).alias("theirs_in_stage2"),
        n(F.col("in_theirs") & F.col("in_stage3")).alias("theirs_in_stage3"),
        n(F.col("high_conf") & F.col("in_stage3")).alias("theirs_r01_in_stage3"),
    ]
    with metrics.job("validation totals"):
        t = joined.agg(*counts).first().asDict()
    t["completeness_stage2"] = round(t["theirs_in_stage2"] / t["theirs"], 4)
    t["completeness_stage3"] = round(t["theirs_in_stage3"] / t["theirs"], 4)
    t["completeness_stage3_r01"] = round(t["theirs_r01_in_stage3"] / t["theirs_r01"], 4)
    t["purity_stage3"] = round(t["theirs_in_stage3"] / t["ours_stage3"], 4)
    metrics.record("stage7", **t)

    with metrics.job("validation by bin"):
        by_bin = joined.groupBy("bin").agg(*counts).orderBy("bin").toPandas()
    by_bin["s_from_au"] = 10 ** (by_bin["bin"] / 2)
    by_bin["completeness_stage3"] = by_bin["theirs_in_stage3"] / by_bin["theirs"]
    by_bin["purity_stage3"] = by_bin["theirs_in_stage3"] / by_bin["ours_stage3"]
    by_bin.to_csv(config.VALIDATION, index=False)
    print(by_bin.to_string(index=False), flush=True)

    with metrics.job("missed pairs"):
        missed = (joined.filter(F.col("in_theirs") & ~F.col("in_stage2"))
                  .select("id_a", "id_b", "eb_s_au", "eb_r", "eb_type").limit(20).toPandas())
    if len(missed):
        missed.to_csv(config.OUTPUT / "stage7_missed_pairs.csv", index=False)
        print(f"  приклади пропущених пар -> stage7_missed_pairs.csv ({len(missed)})", flush=True)
    return t
