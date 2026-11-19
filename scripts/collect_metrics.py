"""Measurement table of several runs from the History Server.

    python3 scripts/collect_metrics.py                     # from the host, History Server on :18080
    python3 scripts/collect_metrics.py --stages 2          # only runs with --stages 2

For every run: variant, stage 2 time (from output/runs/<app_id>.json), input bytes,
shuffle write / read, total task time. Then the median per variant.
With --skip-first the first run of each variant is treated as warm-up.
Standard library only.
"""
import argparse
import json
import statistics
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "output" / "runs"


def get(url):
    """Fetch a URL and return the parsed JSON."""
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def main():
    """Collect metrics of the runs with the requested --stages and print a markdown table.

    For every run the stage 2 time comes from output/runs/<app_id>.json, and bytes and task
    time are summed over all stages from the REST API. At the end the median of each variant is printed.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="http://localhost:18080")
    ap.add_argument("--stages", default="2", help="значення --stages у назві запуску")
    ap.add_argument("--skip-first", action="store_true", help="не рахувати перший запуск кожного варіанту")
    args = ap.parse_args()
    api = f"{args.server}/api/v1"

    rows = []
    for app in sorted(get(f"{api}/applications"), key=lambda a: a["attempts"][0]["startTimeEpoch"]):
        words = app["name"].split()
        # name: "gaia-binaries <input> stages <stages> <variant>"
        if words[0] != "gaia-binaries" or words[-2] != args.stages:
            continue
        stages = get(f"{api}/applications/{app['id']}/stages?status=complete")
        run_file = RUNS / f"{app['id']}.json"
        seconds = json.loads(run_file.read_text()).get("stage2", {}).get("seconds") if run_file.exists() else None
        rows.append({
            "app": app["id"],
            "variant": words[-1],
            "stage2_s": seconds,
            "input_mb": sum(s["inputBytes"] for s in stages) / 1e6,
            "shuffle_write_mb": sum(s["shuffleWriteBytes"] for s in stages) / 1e6,
            "shuffle_read_mb": sum(s["shuffleReadBytes"] for s in stages) / 1e6,
            "task_time_s": sum(s["executorRunTime"] for s in stages) / 1e3,
        })

    if args.skip_first:
        seen, kept = set(), []
        for r in rows:
            if r["variant"] in seen:
                kept.append(r)
            seen.add(r["variant"])
        rows = kept

    cols = ["stage2_s", "input_mb", "shuffle_write_mb", "shuffle_read_mb", "task_time_s"]
    print("| запуск | варіант | " + " | ".join(cols) + " |")
    print("|---|---|" + "---:|" * len(cols))
    for r in rows:
        print(f"| {r['app']} | {r['variant']} | " + " | ".join(fmt(r[c]) for c in cols) + " |")
    print()
    for variant in sorted({r["variant"] for r in rows}):
        group = [r for r in rows if r["variant"] == variant]
        med = [statistics.median(r[c] for r in group if r[c] is not None) if any(r[c] is not None for r in group) else None
               for c in cols]
        print(f"| медіана ({len(group)}) | {variant} | " + " | ".join(fmt(m) for m in med) + " |")


def fmt(x):
    """Format a number with one decimal, spaces between thousands and a decimal comma."""
    return "-" if x is None else f"{x:,.1f}".replace(",", " ").replace(".", ",")


if __name__ == "__main__":
    main()
