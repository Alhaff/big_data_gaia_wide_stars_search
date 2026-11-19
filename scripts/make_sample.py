#!/usr/bin/env python3
"""Sample that runs in 1-2 minutes: a small sky disc at the centre of our region.

    python3 scripts/make_sample.py --raw /media/data/big_data/gaia/raw
    -> sample/GaiaSource_sample.csv.gz (same ECSV format as the full files)

The format is unchanged ('#' metadata, header, all 152 columns), so the pipeline
runs on the sample and on the full data without code changes. Stdlib only.
"""
import argparse
import csv
import gzip
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_gaia import KYIV_LAT, KYIV_LON, ZENITH_TIME, distance, pixel_centre, zenith  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def main():
    """Select stars within --radius of the zenith and write them to --out.

    Only files with at least one HEALPix pixel close to the disc are scanned.
    Metadata and header come from the first file, so the sample has the same format as the full files.
    """
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", type=Path, required=True, help="папка з повними .csv.gz")
    ap.add_argument("--out", type=Path, default=ROOT / "sample" / "GaiaSource_sample.csv.gz")
    ap.add_argument("--radius", type=float, default=2.0, help="радіус круга, градуси (2° ≈ 75 тис. рядків)")
    args = ap.parse_args()

    ra0, dec0 = zenith(ZENITH_TIME, KYIV_LAT, KYIV_LON)
    files = []
    for path in sorted(args.raw.glob("GaiaSource_*.csv.gz")):
        a, b = map(int, path.name[11:-7].split("-"))
        # a file is needed if any of its pixels is closer than radius + pixel size
        if min(distance(ra0, dec0, *pixel_centre(p)) for p in range(a, b + 1)) < args.radius + 0.3:
            files.append(path)
    print(f"центр RA {ra0:.2f}, Dec {dec0:.2f}, радіус {args.radius}°, файлів для перегляду: {len(files)}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    kept = 0
    with gzip.open(args.out, "wt", newline="") as out:
        header_written = False
        for path in files:
            with gzip.open(path, "rt", newline="") as fh:
                meta, line = [], fh.readline()
                while line.startswith("#"):
                    meta.append(line)
                    line = fh.readline()
                columns = next(csv.reader(io.StringIO(line)))
                i_ra, i_dec = columns.index("ra"), columns.index("dec")
                if not header_written:
                    out.writelines(meta)
                    out.write(line)
                    header_written = True
                for row in fh:
                    f = row.split(",", max(i_ra, i_dec) + 1)
                    if distance(ra0, dec0, float(f[i_ra]), float(f[i_dec])) <= args.radius:
                        out.write(row)
                        kept += 1
            print(f"  {path.name}: усього відібрано {kept:,}", flush=True)
    print(f"{kept:,} рядків -> {args.out} ({args.out.stat().st_size / 1e6:.1f} МБ)")


if __name__ == "__main__":
    main()
