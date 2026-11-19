#!/usr/bin/env python3
"""Examples of rows rejected by the stage 1 filter, without Spark.

    python3 scripts/dirty_examples.py                                  # from the sample in sample/
    python3 scripts/dirty_examples.py --input /media/data/big_data/gaia/raw/GaiaSource_093881-097402.csv.gz

For every rule (the same as in pipeline/stage1_filter.py) prints how many rows fail it
and a few examples as a markdown table. Rules are checked independently,
so one row can appear under several rules. Stdlib only.
"""
import argparse
import csv
import gzip
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PARALLAX_MIN, PARALLAX_MAX, PARALLAX_OVER_ERROR_MIN = 1.0, 10.0, 5.0  # as in pipeline/config.py
SHOW = ["source_id", "ra", "dec", "parallax", "parallax_error", "pmra", "pmdec",
        "phot_g_mean_mag", "ruwe", "astrometric_params_solved"]


def num(x):
    """Convert a CSV field to float; an empty field or "null" becomes None."""
    return None if x in ("", "null") else float(x)


# (name as in stage1_filter.rules(), description, condition the row must satisfy)
RULES = [
    ("has_parallax", "немає паралаксу (розв'язок лише з положенням)",
     lambda r: num(r["parallax"]) is not None),
    ("has_proper_motion", "немає власного руху",
     lambda r: num(r["pmra"]) is not None and num(r["pmdec"]) is not None),
    ("has_g_mag", "немає зоряної величини G",
     lambda r: num(r["phot_g_mean_mag"]) is not None),
    ("parallax_over_error", f"ϖ/σ ≤ {PARALLAX_OVER_ERROR_MIN:g} (неточний або від'ємний паралакс)",
     lambda r: num(r["parallax"]) is None or num(r["parallax"]) / num(r["parallax_error"]) > PARALLAX_OVER_ERROR_MIN),
    ("distance_100_1000pc", f"паралакс поза {PARALLAX_MIN:g}-{PARALLAX_MAX:g} мас",
     lambda r: num(r["parallax"]) is None or PARALLAX_MIN <= num(r["parallax"]) <= PARALLAX_MAX),
]


def main():
    """Check every row of the file against all rules and print markdown tables.

    For every rule prints the number and share of rows failing it and up to
    --examples such rows with the main columns.
    """
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", type=Path, default=ROOT / "sample" / "GaiaSource_sample.csv.gz")
    ap.add_argument("--examples", type=int, default=3)
    args = ap.parse_args()

    examples = {name: [] for name, *_ in RULES}
    failed = {name: 0 for name, *_ in RULES}
    total = passed = 0
    with gzip.open(args.input, "rt") as fh:
        for r in csv.DictReader(line for line in fh if not line.startswith("#")):
            total += 1
            ok = True
            for name, _, cond in RULES:
                if not cond(r):
                    ok = False
                    failed[name] += 1
                    if len(examples[name]) < args.examples:
                        examples[name].append(r)
            passed += ok

    print(f"{args.input.name}: рядків {total}, пройшли всі правила {passed} ({passed / total:.1%})\n")
    print("| Правило | Що означає | Не пройшли | Частка |")
    print("|---|---|---:|---:|")
    for name, text, _ in RULES:
        print(f"| `{name}` | {text} | {failed[name]} | {failed[name] / total:.1%} |")
    for name, text, _ in RULES:
        print(f"\n**`{name}`**: {text}\n")
        cols = [c for c in SHOW if examples[name] and c in examples[name][0]]
        print("| " + " | ".join(f"`{c}`" for c in cols) + " |")
        print("|" + "---|" * len(cols))
        for r in examples[name]:
            print("| " + " | ".join(r[c] for c in cols) + " |")


if __name__ == "__main__":
    main()
