"""The whole pipeline in one command.

    spark-submit pipeline/run_all.py --input /work/sample                # sample
    spark-submit pipeline/run_all.py --input /data/raw                   # full dataset
    spark-submit pipeline/run_all.py --input /data/raw --stages 2-3      # stages 2-3 only
    spark-submit pipeline/run_all.py --input /data/raw --stages 2 --no-measure --all-neighbours

Stages that are not run take the results of earlier stages from Parquet in output/.
"""
import argparse

from pipeline import (config, metrics, stage1_filter, stage2_pairs, stage3_neighbours,
                      stage4_systems, stage5_chance, stage6_stats, stage7_validate)


def parse_stages(text):
    """Turn a string like "2-5" or "4" into a set of stage numbers, {2, 3, 4, 5} or {4}."""
    a, _, b = text.partition("-")
    return set(range(int(a), int(b or a) + 1))


def main():
    """Parse the command line arguments and run the selected stages in order."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="папка з .csv.gz")
    ap.add_argument("--stages", default="1-6", help="наприклад 1-6, 2-3 або 4")
    ap.add_argument("--no-measure", action="store_true",
                    help="без додаткових job-ів вимірювання (для чистих замірів часу)")
    ap.add_argument("--all-neighbours", action="store_true",
                    help="етап 2 копіює зірки в усі 8 сусідніх комірок")
    args = ap.parse_args()
    stages = parse_stages(args.stages)

    variant = "all_neighbours" if args.all_neighbours else "lower_neighbours"
    spark = config.spark(f"gaia-binaries {args.input} stages {args.stages} {variant}")
    metrics.set_app_id(spark.sparkContext.applicationId)

    def read(path):
        return spark.read.parquet(str(path))

    if 1 in stages:
        with metrics.stage("stage1"):
            stage1_filter.run(spark, args.input)
    if 2 in stages:
        with metrics.stage("stage2"):
            stage2_pairs.run(spark, read(config.STARS), measure=not args.no_measure,
                             all_neighbours=args.all_neighbours)
    if 3 in stages:
        with metrics.stage("stage3"):
            stage3_neighbours.run(spark, read(config.PAIRS))
    if 4 in stages:
        with metrics.stage("stage4"):
            stage4_systems.run(spark, read(config.PAIRS_CLEAN))
    if 5 in stages:
        with metrics.stage("stage5"):
            stage5_chance.run(spark, read(config.STARS))
    if 6 in stages:
        with metrics.stage("stage6"):
            stage6_stats.run(spark, read(config.PAIRS_CLEAN), read(config.PAIRS_SHIFTED))
    if 7 in stages:
        with metrics.stage("stage7"):
            stage7_validate.run(spark, read(config.PAIRS), read(config.PAIRS_CLEAN))

    spark.stop()


if __name__ == "__main__":
    main()
