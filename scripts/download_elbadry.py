#!/usr/bin/env python3
"""Download the El-Badry, Rix & Heintz (2021) wide-binary catalog for stage 7 (validation).

    python3 scripts/download_elbadry.py                                   # into data/elbadry
    python3 scripts/download_elbadry.py --path /media/data/big_data/elbadry

Source: https://zenodo.org/records/4435257 (CC BY 4.0), file all_columns_catalog.fits.gz
(1.42 GB, 1,817,594 pairs over the whole sky). File name, size and MD5 come from the Zenodo API.
Rerunning resumes a partial download (.part) and skips a finished one.  Stdlib only.
"""
import argparse
import hashlib
import json
import sys
import time
import urllib.request
from pathlib import Path

RECORD = "https://zenodo.org/api/records/4435257"
FILE = "all_columns_catalog.fits.gz"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "elbadry"


def md5sum(path):
    """Return the hex MD5 of a file, read in 4 MB chunks."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 22):
            h.update(chunk)
    return h.hexdigest()


def main():
    """Download the catalog from Zenodo into --path and verify its MD5.

    File name, size and MD5 come from the Zenodo API. A partial download is resumed,
    a finished one is skipped. On success manifest.json is written next to the file.
    """
    ap = argparse.ArgumentParser(description="Download the El-Badry et al. (2021) catalog.")
    ap.add_argument("--path", type=Path, default=DATA_DIR, help=f"target directory (default: {DATA_DIR})")
    args = ap.parse_args()
    args.path.mkdir(parents=True, exist_ok=True)

    with urllib.request.urlopen(RECORD, timeout=60) as r:
        info = next(f for f in json.load(r)["files"] if f["key"] == FILE)
    url, size, md5 = info["links"]["self"], info["size"], info["checksum"].removeprefix("md5:")

    dst = args.path / FILE
    if dst.exists():
        print(f"{FILE} already downloaded")
        return
    part = dst.with_name(FILE + ".part")
    print(f"downloading {FILE} ({size / 1e9:.2f} GB) -> {args.path}", flush=True)
    for attempt in range(1, 6):
        try:
            have = part.stat().st_size if part.exists() else 0
            if have < size:  # resume from where the last attempt stopped
                req = urllib.request.Request(url, headers={"Range": f"bytes={have}-"})
                with urllib.request.urlopen(req, timeout=120) as r, open(part, "ab") as out:
                    while chunk := r.read(1 << 20):
                        out.write(chunk)
            if md5sum(part) == md5:
                part.rename(dst)
                (args.path / "manifest.json").write_text(json.dumps(
                    {"source": "https://zenodo.org/records/4435257", "license": "CC BY 4.0",
                     "file": FILE, "size": size, "md5": md5}, indent=1))
                print(f"finished {FILE}, MD5 ok")
                return
            print("MD5 mismatch, starting over", flush=True)
            part.unlink()
        except OSError as e:
            print(f"retry {attempt}/5: {e}", flush=True)
            time.sleep(2 ** attempt)
    sys.exit(f"FAILED {FILE}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)  # the .part file stays; rerun to resume
