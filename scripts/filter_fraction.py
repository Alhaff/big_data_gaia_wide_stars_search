#!/usr/bin/env python3
"""Estimate of the share of rows passing the stage 1 filter, without Spark.

    python3 scripts/filter_fraction.py --data /media/data/big_data/gaia
    python3 scripts/filter_fraction.py --data /media/data/big_data/gaia --rows 50000

Reads the first --rows rows of every file, applies the same rules as
pipeline/stage1_filter.py and weights the shares by the file row counts from measurements.json.
Stdlib only. The first rows cover only part of a file's sky region, so the result is an estimate.
"""
import argparse
import csv
import gzip
import json
from pathlib import Path

PARALLAX_MIN, PARALLAX_MAX, PARALLAX_OVER_ERROR_MIN = 1.0, 10.0, 5.0


def num(x):
    """Convert a CSV field to float; an empty field or "null" becomes None."""
    return None if x in ("", "null") else float(x)


def passes(r):
    """Check whether a row passes all rules of the stage 1 filter."""
    plx, err = num(r["parallax"]), num(r["parallax_error"])
    if plx is None or num(r["pmra"]) is None or num(r["pmdec"]) is None or num(r["phot_g_mean_mag"]) is None:
        return False
    return plx / err > PARALLAX_OVER_ERROR_MIN and PARALLAX_MIN <= plx <= PARALLAX_MAX


def main():
    """Read the first --rows rows of every file and print the share passing the filter.

    File shares are weighted by the row counts from measurements.json; at the end
    the weighted share and the estimated number of stars N are printed.
    """
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
