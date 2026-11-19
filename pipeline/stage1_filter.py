"""Stage 1. Read the CSV files, drop dirty records, filter stars, write Parquet. No shuffle.

Input:  .csv.gz files in ECSV format ('#' metadata lines, then CSV with 152 columns)
Output: stage1_stars.parquet - 12 columns plus the cell index and origin = 0
Each file is read by one task, because gzip cannot be split.
"""
import functools

from pyspark.sql import functions as F

from pipeline import config, metrics


def rules():
    """Return the star selection rules as a list of (name, condition the row must satisfy).

    The first three rules drop records with empty fields, the rest select by
    parallax quality and distance. This is a function rather than a module constant
    because in PySpark 4 F.col() can only be called after the SparkContext exists.
    """
    return [
        ("has_parallax", F.col("parallax").isNotNull()),
        ("has_proper_motion", F.col("pmra").isNotNull() & F.col("pmdec").isNotNull()),
        ("has_g_mag", F.col("phot_g_mean_mag").isNotNull()),
        ("parallax_over_error", F.col("parallax") / F.col("parallax_error") > config.PARALLAX_OVER_ERROR_MIN),
        ("distance_100_1000pc", F.col("parallax").between(config.PARALLAX_MIN, config.PARALLAX_MAX)),
    ]


def run(spark, input_path):
    """Read the raw Gaia files, filter the stars and write them to config.STARS.

    The number of rows failing each rule is counted in a single aggregation
    and recorded in the metrics. Column types are set explicitly instead of
    being inferred, which would cost an extra pass over the data.
    Returns a DataFrame of the written stars.
    """
    rule_list = rules()
    raw = spark.read.csv(input_path, header=True, comment="#", nullValue="null")
    stars = raw.select([F.col(c).cast("long" if c == "source_id" else "double") for c in config.COLUMNS])

    with metrics.job("count rejected rows"):
        rejected = stars.agg(
            F.count("*").alias("total"),
            *[F.sum(F.when(~cond, 1).otherwise(0)).alias(name) for name, cond in rule_list],
        ).first()
    metrics.record("stage1", **rejected.asDict())

    good = stars.filter(functools.reduce(lambda a, b: a & b, [cond for _, cond in rule_list]))
    good = good.withColumn("cell", F.shiftright("source_id", config.CELL_SHIFT)).withColumn("origin", F.lit(0))
    metrics.save_plan(good, "stage1")

    with metrics.job("write stars"):
        good.write.mode("overwrite").parquet(str(config.STARS))

    out = spark.read.parquet(str(config.STARS))
    with metrics.job("count stars"):
        metrics.record("stage1", stars_out=out.count())
    return out
