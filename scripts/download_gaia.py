#!/usr/bin/env python3
"""Download a patch of Gaia DR3 `gaia_source`: the sky above Kyiv at midnight, 31 January.

    python3 scripts/download_gaia.py          # select files, download, measure
    python3 scripts/download_gaia.py --plan   # only select files and print the estimate
    python3 scripts/download_gaia.py --path /media/data/big_data/gaia   # other data dir

How files are chosen
--------------------
Bulk files are named GaiaSource_AAAAAA-BBBBBB.csv.gz: an inclusive range of HEALPix
level-8 pixels (nside=256, NESTED), so we know where each file lies on the sky.
The zenith has Dec = latitude of Kyiv and RA = local sidereal time.  Files are sorted by
the median distance of their pixels from the zenith (i.e. "half the file is within r"),
and taken nearest-first until the estimated row count reaches TARGET_ROWS.

Output (in data/, or --path): raw/*.csv.gz, manifest.json (files + MD5), measurements.json.
Rerunning resumes: finished files are skipped.  Stdlib only.
"""
import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import math
import re
import sys
import time
from functools import partial
import urllib.request
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

# ---- settings ---------------------------------------------------------------
ZENITH_TIME = dt.datetime.fromisoformat("2026-01-31T00:00:00+02:00")  # midnight, Kyiv
KYIV_LAT, KYIV_LON = 50.45, 30.52
TARGET_ROWS = 12e6          # course needs >= 10M; keep a margin
DATA_DIR = Path(__file__).resolve().parent.parent / "data"  # default; override with --path

BASE_URL = "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/"
LIST_URL = "https://gaia.eu-1.cdn77-storage.com/?prefix=Gaia/gdr3/gaia_source/&delimiter=/"
TOTAL_ROWS = 1_811_709_771  # whole table, to turn file size into a row estimate
NSIDE = 256


# ---- sky geometry -------------------------------------------------------------
def pixel_centre(pix):
    """(ra, dec) in degrees of a NESTED HEALPix pixel centre (port of healpix_base)."""
    def every_second_bit(v):
        r = b = 0
        while v:
            r |= (v & 1) << b
            v >>= 2
            b += 1
        return r

    face, ipf = divmod(pix, NSIDE * NSIDE)
    x, y = every_second_bit(ipf), every_second_bit(ipf >> 1)
    jr = [2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4][face] * NSIDE - x - y - 1
    if jr < NSIDE:
        nr, z, shift = jr, 1 - jr * jr / (3 * NSIDE * NSIDE), 0
    elif jr > 3 * NSIDE:
        nr = 4 * NSIDE - jr
        z, shift = nr * nr / (3 * NSIDE * NSIDE) - 1, 0
    else:
        nr, z, shift = NSIDE, (2 * NSIDE - jr) * 2 / (3 * NSIDE), (jr - NSIDE) & 1
    jp = ([1, 3, 5, 7, 0, 2, 4, 6, 1, 3, 5, 7][face] * nr + x - y + 1 + shift) // 2
    jp = (jp - 1) % (4 * NSIDE) + 1
    phi = (jp - (shift + 1) / 2) * (math.pi / 2 / nr)
    return math.degrees(phi), math.degrees(math.asin(z))


def zenith(when, lat, lon):
    """(ra, dec) of the zenith: RA = local sidereal time."""
    jd = when.timestamp() / 86400 + 2440587.5
    lst_hours = 18.697374558 + 24.06570982441908 * (jd - 2451545.0) + lon / 15
    return (lst_hours % 24) * 15, lat


def distance(ra1, dec1, ra2, dec2):
    """Angle between two sky points, degrees."""
    a1, d1, a2, d2 = map(math.radians, (ra1, dec1, ra2, dec2))
    c = math.sin(d1) * math.sin(d2) + math.cos(d1) * math.cos(d2) * math.cos(a1 - a2)
    return math.degrees(math.acos(max(-1.0, min(1.0, c))))


# ---- file list and selection --------------------------------------------------
def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "gaia-course-downloader"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def list_files():
    md5 = {name: h for h, name in (line.split() for line in get(BASE_URL + "_MD5SUM.txt").decode().splitlines())}
    files, marker = [], ""
    while True:  # S3-style listing, 1000 keys per page
        xml = get(LIST_URL + "&marker=" + marker).decode()
        for name, a, b, size in re.findall(
                r"(GaiaSource_(\d+)-(\d+)\.csv\.gz)</Key>.*?<Size>(\d+)", xml, re.S):
            files.append({"name": name, "md5": md5[name], "size_gz": int(size),
                          "pix_first": int(a), "pix_last": int(b)})
        if "<IsTruncated>true" not in xml:
            return files
        marker = re.search(r"<NextMarker>([^<]+)", xml).group(1)


def select(files, ra0, dec0):
    rows_per_byte = TOTAL_ROWS / sum(f["size_gz"] for f in files)
    for f in files:
        f["est_rows"] = int(f["size_gz"] * rows_per_byte)
        d = sorted(distance(ra0, dec0, *pixel_centre(p)) for p in range(f["pix_first"], f["pix_last"] + 1))
        f["median_dist"] = d[len(d) // 2]
    files.sort(key=lambda f: f["median_dist"])
    chosen, rows = [], 0
    for f in files:  # nearest first until the target is reached
        chosen.append(f)
        rows += f["est_rows"]
        if rows >= TARGET_ROWS:
            return chosen


# ---- download and measure -----------------------------------------------------
def say(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def download(f, raw_dir):
    dst = raw_dir / f["name"]
    if dst.exists():
        return "skip"
    part = dst.with_name(dst.name + ".part")
    for attempt in range(1, 6):
        try:
            have = part.stat().st_size if part.exists() else 0
            if have < f["size_gz"]:  # resume from where the last attempt stopped
                req = urllib.request.Request(BASE_URL + f["name"], headers={"Range": f"bytes={have}-"})
                with urllib.request.urlopen(req, timeout=120) as r, open(part, "ab") as out:
                    while chunk := r.read(1 << 20):
                        out.write(chunk)
            if md5sum(part) == f["md5"]:
                part.rename(dst)
                say(f"finished {f['name']} ({f['size_gz'] / 1e6:.0f} MB)")
                return "ok"
            part.unlink()  # corrupted: start over
        except OSError as e:
            say(f"retry {attempt}/5 {f['name']}: {e}")
            time.sleep(2 ** attempt)
    say(f"FAILED {f['name']}")
    return "fail"


def measure(path):
    """Rows and uncompressed bytes. Lines starting with '#' are ECSV metadata;
    the first other line holds the column names (hence rows starts at -1)."""
    rows, raw = -1, 0
    with gzip.open(path, "rb") as fh:
        for line in fh:
            raw += len(line)
            if not line.startswith(b"#"):
                rows += 1
    return {"name": path.name, "rows": rows, "raw_bytes": raw}


def main():
    ap = argparse.ArgumentParser(description="Download Gaia DR3 files for the sky above Kyiv.")
    ap.add_argument("--path", type=Path, default=DATA_DIR,
                    help=f"data directory (default: {DATA_DIR})")
    ap.add_argument("--plan", action="store_true", help="only select files and write manifest.json")
    args = ap.parse_args()
    data_dir = args.path.expanduser().resolve()
    raw_dir = data_dir / "raw"
    data_dir.mkdir(parents=True, exist_ok=True)

    ra0, dec0 = zenith(ZENITH_TIME, KYIV_LAT, KYIV_LON)
    say(f"Zenith over Kyiv at {ZENITH_TIME}: RA {ra0:.2f}, Dec {dec0:.2f}")
    files = select(list_files(), ra0, dec0)
    gz = sum(f["size_gz"] for f in files)
    rows = sum(f["est_rows"] for f in files)
    say(f"{len(files)} files within {files[-1]['median_dist']:.1f} deg, {gz / 1e9:.2f} GB gz, "
          f"~{rows / 1e6:.1f}M rows (estimate)")

    keep = ("name", "md5", "size_gz", "pix_first", "pix_last")
    (data_dir / "manifest.json").write_text(json.dumps(
        {"zenith_time": ZENITH_TIME.isoformat(), "centre_radec": [ra0, dec0], "source": BASE_URL,
         "files": [{k: f[k] for k in keep} for f in files]}, indent=1))
    if args.plan:
        return

    raw_dir.mkdir(exist_ok=True)
    say(f"downloading to {raw_dir}")
    try:
        with ThreadPoolExecutor(4) as pool:
            results = list(pool.map(partial(download, raw_dir=raw_dir), files))
    except KeyboardInterrupt:  # threads can't be interrupted; exit now, .part files resume next run
        print("\ninterrupted - rerun to resume", flush=True)
        os._exit(130)
    say(f"downloads done: {results.count('ok')} new, {results.count('skip')} already present")
    if "fail" in results:
        sys.exit("some downloads failed; rerun to resume")

    say("measuring rows and uncompressed size ...")
    with ProcessPoolExecutor() as pool:
        stats = list(pool.map(measure, [raw_dir / f["name"] for f in files]))
    total_rows = sum(s["rows"] for s in stats)
    total_raw = sum(s["raw_bytes"] for s in stats)
    (data_dir / "measurements.json").write_text(json.dumps(
        {"rows": total_rows, "raw_bytes": total_raw, "files": stats}, indent=1))
    say(f"Measured: {total_rows:,} rows, {total_raw / 1e9:.2f} GB uncompressed")


if __name__ == "__main__":
    main()
