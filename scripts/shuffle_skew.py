#!/usr/bin/env python3
"""Skew of every shuffle from a Spark event log, without the History Server or Spark.

    python3 scripts/shuffle_skew.py                                   # log from evidence/
    python3 scripts/shuffle_skew.py --log evidence/spark-events/eventlog_v2_local-1794941788737

For every stage that reads a shuffle, computes the distribution over tasks (one task = one or
more shuffle partitions after AQE coalescing): records and bytes per task (max, median, share of
the top 10 tasks if there are more than 10) and task run time (max / median). This is the cost of
skew in the run: if max/median is close to 1, the longest task does not delay the stage.
Stdlib; .zstd is unpacked with the zstd tool.
"""
import argparse
import json
import statistics
import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def events(log_dir):
    """Read the Spark event log in log_dir and yield events one by one as dicts."""
    for f in sorted(Path(log_dir).glob("events_*")):
        if f.suffix == ".zstd":
            text = subprocess.run(["zstd", "-dc", str(f)], capture_output=True, check=True, text=True).stdout
        else:
            text = f.read_text()
        for line in text.splitlines():
            if line:
                yield json.loads(line)


def fmt(x):
    """Format an integer with spaces between thousands."""
    return f"{x:,.0f}".replace(",", " ")


def main():
    """Compute the task distribution of stages that read a shuffle and print a markdown table.

    For every stage takes the job description, the number of tasks, shuffle read records
    and bytes, and the run time of each task.
    """
    default = next((ROOT / "evidence" / "spark-events").glob("eventlog_v2_*"), None)
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", type=Path, default=default)
    ap.add_argument("--min-tasks", type=int, default=2, help="пропускати stage-і з меншою кількістю задач")
    args = ap.parse_args()

    stage_job = {}          # stage id -> job description
    stage_name = {}
    tasks = defaultdict(list)  # stage id -> [(records, bytes, run_ms)]
    for e in events(args.log):
        kind = e["Event"]
        if kind == "SparkListenerJobStart":
            desc = (e.get("Properties") or {}).get("spark.job.description", "")
            for s in e["Stage IDs"]:
                stage_job.setdefault(s, desc)
        elif kind == "SparkListenerStageCompleted":
            stage_name[e["Stage Info"]["Stage ID"]] = e["Stage Info"]["Stage Name"]
        elif kind == "SparkListenerTaskEnd" and e.get("Task Metrics"):
            m = e["Task Metrics"]
            r = m.get("Shuffle Read Metrics") or {}
            rec = r.get("Total Records Read", 0)
            b = r.get("Remote Bytes Read", 0) + r.get("Local Bytes Read", 0)
            if rec or b:
                tasks[e["Stage ID"]].append((rec, b, m.get("Executor Run Time", 0)))

    print(f"журнал: {args.log.name}\n")
    print("| Stage | Job | Задач | Записів: max / медіана | Частка топ-10 | Байтів: max / медіана, МБ | Час задачі: max / медіана, с |")
    print("|---:|---|---:|---:|---:|---:|---:|")
    for s in sorted(tasks):
        t = tasks[s]
        if len(t) < args.min_tasks:
            continue
        rec = sorted((x[0] for x in t), reverse=True)
        byt = sorted(x[1] for x in t)
        run = sorted(x[2] for x in t)
        top10 = sum(rec[:10]) / sum(rec) if sum(rec) else 0
        print(f"| {s} | {stage_job.get(s, '?')} | {len(t)} "
              f"| {fmt(rec[0])} / {fmt(statistics.median(rec))} "
              + (f"| {top10:.1%} ".replace(".", ",") if len(t) > 10 else "| - ")
              + f"| {byt[-1] / 1e6:.1f} / {statistics.median(byt) / 1e6:.1f} ".replace(".", ",")
              + f"| {run[-1] / 1e3:.1f} / {statistics.median(run) / 1e3:.1f} |".replace(".", ","))
    print("\nСтадії з 1 задачею (злиті AQE в один розділ) пропущено; --min-tasks 1 показує всі.")


if __name__ == "__main__":
    main()
