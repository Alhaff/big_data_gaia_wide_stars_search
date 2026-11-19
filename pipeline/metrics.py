"""Measurements: stage timings, execution plans, key distribution, job labels in the Spark UI."""
import contextlib
import io
import json
import time

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline import config

_results = {}
_app_id = None
_stage = None


def set_app_id(app_id: str):
    """Remember the Spark application id and record it in the metrics.

    From then on the metrics are also saved to output/runs/<app_id>.json,
    so results of several runs do not overwrite each other.
    """
    global _app_id
    _app_id = app_id
    record("run", app_id=app_id)


@contextlib.contextmanager
def stage(name: str):
    """Context of a pipeline stage: time it and save the metrics when it ends.

    Jobs that Spark starts on its own during the stage (for example, reading
    a Parquet schema) are labelled "<stage> - auto" in the Spark UI.
    Our own actions are labelled with job().
    """
    global _stage
    _stage = name
    _describe(f"{name} - auto")
    t0 = time.time()
    print(f"\n==== {name} ====", flush=True)
    yield _results.setdefault(name, {})
    _describe(None)
    _stage = None
    _results[name]["seconds"] = round(time.time() - t0, 1)
    print(f"==== {name}: {_results[name]['seconds']} с ====", flush=True)
    save()


@contextlib.contextmanager
def job(label: str):
    """Label the jobs inside the context as "<stage> - <label>" in the Spark UI.

    Jobs that AQE starts during this action get the same label, because Spark
    copies thread-local properties into its background threads. On exit
    the label goes back to "<stage> - auto".
    """
    _describe(f"{_stage} - {label}" if _stage else label)
    try:
        yield
    finally:
        _describe(f"{_stage} - auto" if _stage else None)


def _describe(text):
    sc = SparkContext._active_spark_context
    if sc:
        sc.setJobDescription(text)


def record(name: str, **values):
    """Add values to the metrics section name and print each on its own line."""
    _results.setdefault(name, {}).update(values)
    for k, v in values.items():
        print(f"  {k}: {v}", flush=True)


def save():
    """Write all metrics to output/metrics.json and, if app_id is known, to output/runs/<app_id>.json."""
    config.OUTPUT.mkdir(parents=True, exist_ok=True)
    text = json.dumps(_results, indent=1, ensure_ascii=False, default=str)
    config.METRICS.write_text(text)
    if _app_id:
        config.RUNS.mkdir(parents=True, exist_ok=True)
        (config.RUNS / f"{_app_id}.json").write_text(text)


def save_plan(df: DataFrame, name: str):
    """Write explain("formatted") to output/plans/<name>.txt and print the shuffle and broadcast counts.

    This is the plan before execution: AQE may change it at runtime, the final plan
    is in the Spark UI on the SQL tab. Shuffles are counted by the lines
    "Arguments: hashpartitioning(", broadcasts by BroadcastExchange nodes.
    """
    config.PLANS.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain("formatted")
    text = buf.getvalue()
    (config.PLANS / f"{name}.txt").write_text(text)
    shuffles = text.count("Arguments: hashpartitioning(")
    broadcasts = text.count(") BroadcastExchange\n")
    print(f"  план -> plans/{name}.txt, shuffle (hashpartitioning): {shuffles}, broadcast: {broadcasts}", flush=True)


def key_distribution(df: DataFrame, key: str, name: str):
    """Compute the distribution of rows per shuffle key and record it in the metrics.

    Records the number of keys and rows, the largest key, the median and the share
    of the 10 largest keys. Runs separate jobs with their own groupBy.
    """
    counts = df.groupBy(key).count()
    s = counts.agg(
        F.count("*").alias("keys"),
        F.sum("count").alias("rows"),
        F.max("count").alias("max"),
        F.percentile_approx("count", 0.5).alias("median"),
    ).first()
    top10 = counts.orderBy(F.desc("count")).limit(10).agg(F.sum("count")).first()[0]
    record(name, keys=s["keys"], rows=s["rows"], max=s["max"], median=s["median"],
           top10_share=round(top10 / s["rows"], 4))
