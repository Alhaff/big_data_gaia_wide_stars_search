#!/usr/bin/env python3
"""Яка частка рядків пройде фільтр етапу 1. Для оцінок у розділі 3 звіту, без Spark.

    python3 scripts/filter_fraction.py --data /media/data/big_data/gaia
    python3 scripts/filter_fraction.py --data /media/data/big_data/gaia --rows 50000

З кожного файлу читає перші --rows рядків, застосовує ті самі правила, що й
pipeline/stage1_filter.py, і зважує частки за кількістю рядків файлу з measurements.json.
Лише stdlib. Перші рядки файлу - це лише частина його ділянки неба, тож результат - оцінка.
"""
import argparse
import csv
import gzip
import json
from pathlib import Path

PARALLAX_MIN, PARALLAX_MAX, PARALLAX_OVER_ERROR_MIN = 1.0, 10.0, 5.0


def num(x):
    return None if x in ("", "null") else float(x)


def passes(r):
    plx, err = num(r["parallax"]), num(r["parallax_error"])
    if plx is None or num(r["pmra"]) is None or num(r["pmdec"]) is None or num(r["phot_g_mean_mag"]) is None:
        return False
    return plx / err > PARALLAX_OVER_ERROR_MIN and PARALLAX_MIN <= plx <= PARALLAX_MAX


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", type=Path, required=True, help="папка з raw/ і measurements.json")
    ap.add_argument("--rows", type=int, default=20_000, help="скільки рядків читати з кожного файлу")
    args = ap.parse_args()

    files = json.loads((args.data / "measurements.json").read_text())["files"]
    total_rows = total_pass = 0.0
    for f in sorted(files, key=lambda f: f["name"]):
        with gzip.open(args.data / "raw" / f["name"], "rt") as fh:
            reader = csv.DictReader(line for line in fh if not line.startswith("#"))
            seen = ok = 0
            for r in reader:
                seen += 1
                ok += passes(r)
                if seen >= args.rows:
                    break
        share = ok / seen
        total_rows += f["rows"]
        total_pass += f["rows"] * share
        print(f"{f['name']}: {ok} / {seen} = {share:.1%}")

    print(f"\nзважена частка: {total_pass / total_rows:.1%}, "
          f"N ≈ {total_pass:,.0f} з {total_rows:,.0f}".replace(",", " "))


if __name__ == "__main__":
    main()
