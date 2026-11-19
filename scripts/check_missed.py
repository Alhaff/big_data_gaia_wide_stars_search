"""Why pairs from the El-Badry catalog were not found on our stage 2. Run in the container:

    podman compose run --rm spark python3 scripts/check_missed.py

Input: output/stage7_missed_pairs.csv (stage 7), output/stage1_stars.parquet.
For every pair: whether both stars passed the stage 1 filter, whether their HEALPix cells
are neighbours, and which of the three pair criteria fails (same formulas as pipeline/geometry.py).
No Spark: pandas, pyarrow and healpy only.
"""
import sys
from pathlib import Path

import healpy as hp
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline import config, geometry  # noqa: E402

missed = pd.read_csv(config.OUTPUT / "stage7_missed_pairs.csv")
ids = set(missed["id_a"]) | set(missed["id_b"])
stars = pq.read_table(config.STARS, filters=[("source_id", "in", list(ids))]).to_pandas().set_index("source_id")

rows = []
for p in missed.itertuples():
    r = {"id_a": p.id_a, "id_b": p.id_b, "eb_s_au": round(p.eb_s_au)}
    have = [i in stars.index for i in (p.id_a, p.id_b)]
    r["in_stage1"] = f"{int(have[0])}{int(have[1])}"
    if all(have):
        a, b = stars.loc[p.id_a], stars.loc[p.id_b]
        # cells: same, neighbouring or far
        ca, cb = int(a["cell"]), int(b["cell"])
        r["cells"] = "same" if ca == cb else ("nb" if cb in hp.get_all_neighbours(config.NSIDE, ca, nest=True) else "FAR")
        # criteria as in geometry._pairs
        v = geometry.unit_vectors(np.array([a["ra"], b["ra"]]), np.array([a["dec"], b["dec"]]))
        theta = geometry.angle_arcsec(v[:1], v[1:])[0]
        plx1 = a["parallax"] if a["phot_g_mean_mag"] <= b["phot_g_mean_mag"] else b["parallax"]
        r["theta"] = round(theta, 2)
        r["s_ours"] = round(theta * 1000 / plx1)
        r["ok_theta"] = theta < config.S_MAX_AU * plx1 / 1000
        nsig = config.PARALLAX_N_SIGMA_CLOSE if theta < config.CLOSE_THETA_ARCSEC else config.PARALLAX_N_SIGMA
        dplx = abs(a["parallax"] - b["parallax"]) / np.hypot(a["parallax_error"], b["parallax_error"])
        r["dplx_sigma"] = round(dplx, 2)
        r["ok_plx"] = dplx < nsig
        dra, ddec = a["pmra"] - b["pmra"], a["pmdec"] - b["pmdec"]
        dmu = np.hypot(dra, ddec)
        sig = np.sqrt((dra**2 * (a["pmra_error"]**2 + b["pmra_error"]**2)
                       + ddec**2 * (a["pmdec_error"]**2 + b["pmdec_error"]**2)) / max(dmu, 1e-12)**2)
        limit = config.DMU_ORBIT_COEF * plx1**1.5 / np.sqrt(theta) + config.DMU_N_SIGMA * sig
        r["dmu/limit"] = round(dmu / limit, 3)
        r["ok_pm"] = dmu <= limit
        r["same_g"] = a["phot_g_mean_mag"] == b["phot_g_mean_mag"]
    rows.append(r)

out = pd.DataFrame(rows)
pd.set_option("display.width", 200)
print(out.to_string(index=False))
print()
print("обидві зірки пройшли етап 1:", (out["in_stage1"] == "11").sum(), "з", len(out))
if "cells" in out:
    print("комірки:", out["cells"].value_counts().to_dict())
    for c in ("ok_theta", "ok_plx", "ok_pm"):
        print(f"{c} не виконано:", (out[c] == False).sum())  # noqa: E712
