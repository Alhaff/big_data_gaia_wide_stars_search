#!/usr/bin/env python3
"""Check the downloaded Gaia files against dataset/MD5SUM.txt. Portable replacement for `md5sum -c`.

    python3 scripts/verify_md5.py data/raw
    python3 scripts/verify_md5.py /media/data/big_data/gaia/raw

Prints one line per file and exits with code 1 if any file is missing or differs. Stdlib only.
"""
import hashlib
import sys
from pathlib import Path

SUMS = Path(__file__).resolve().parent.parent / "dataset" / "MD5SUM.txt"


def md5sum(path):
    """Return the hex MD5 of a file, read in 4 MB chunks."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 22):
            h.update(chunk)
    return h.hexdigest()


def main():
    """Compare every file listed in MD5SUM.txt with its copy in the given folder."""
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    raw = Path(sys.argv[1])
    bad = 0
    for line in SUMS.read_text().splitlines():
        expected, name = line.split()
        path = raw / name
        if not path.exists():
            status = "MISSING"
        else:
            status = "OK" if md5sum(path) == expected else "FAILED"
        bad += status != "OK"
        print(f"{name}: {status}", flush=True)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
