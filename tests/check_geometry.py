"""Check of pipeline/geometry.py on synthetic data, without Spark.

    podman compose run --rm spark python3 tests/check_geometry.py

What is checked:
1. The cell search (add_copies + grouping by cell + pairs_in_cell) finds exactly
   the same pairs as brute force over all pairs (cross-checked with cKDTree candidates by angle).
2. Every pair is found once (no duplicates).
3. Almost all injected "real" pairs are found.
4. Copies into all 8 neighbours give the same pairs after deduplication.
"""
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from pipeline import config, geometry

rng = np.random.default_rng(42)
RA0, DEC0, RADIUS = 130.67, 50.45, 3.0
N_FIELD, N_BINARIES = 1500, 200   # brute force: ≈1.8M candidates, ≈0.5 GB of memory


def random_stars(n):
    """Return n random stars in a square around the region centre, with parallax and proper motion."""
    return pd.DataFrame({
        "ra": RA0 + rng.uniform(-RADIUS, RADIUS, n) / np.cos(np.radians(DEC0)),
        "dec": DEC0 + rng.uniform(-RADIUS, RADIUS, n),
        "parallax": rng.uniform(1, 10, n),
        "pmra": rng.normal(0, 20, n),
        "pmdec": rng.normal(0, 20, n),
    })


field = random_stars(N_FIELD)
primaries = random_stars(N_BINARIES)
# companion: within 80% of θ_max, same parallax and almost the same proper motion
theta_max_deg = config.S_MAX_AU * primaries["parallax"] / 1000 / 3600
r = rng.uniform(0.05, 0.8, N_BINARIES) * theta_max_deg
phi = rng.uniform(0, 2 * np.pi, N_BINARIES)
companions = primaries.assign(
    ra=primaries["ra"] + r * np.cos(phi) / np.cos(np.radians(primaries["dec"])),
    dec=primaries["dec"] + r * np.sin(phi),
    pmra=primaries["pmra"] + rng.normal(0, 0.05, N_BINARIES),
    pmdec=primaries["pmdec"] + rng.normal(0, 0.05, N_BINARIES),
)
stars = pd.concat([field, primaries, companions], ignore_index=True)
n = len(stars)
stars = stars.assign(
    source_id=np.arange(n, dtype=np.int64) + 1,
    parallax_error=0.05, pmra_error=0.05, pmdec_error=0.05,
    phot_g_mean_mag=rng.uniform(10, 20, n), bp_rp=1.0, ruwe=1.0, origin=0,
)
stars["cell"] = geometry.cell_of(stars["ra"].to_numpy(), stars["dec"].to_numpy())
injected = {(i + 1 + N_FIELD, i + 1 + N_FIELD + N_BINARIES) for i in range(N_BINARIES)}

# ---- 1. via cells, as in Spark: mapInPandas(add_copies) + groupBy(cell).applyInPandas
copies = pd.concat(geometry.add_copies([stars]), ignore_index=True)
by_cells = pd.concat([geometry.pairs_in_cell(g) for _, g in copies.groupby("cell")], ignore_index=True)
print(f"зірок {n}, комірок {stars['cell'].nunique()}, рядків після копій {len(copies)} "
      f"({len(copies) / n:.2f} на зірку, очікуємо ≈5)")

# ---- 2. brute force: all stars as one "cell"; angle candidates independently via cKDTree
brute = geometry.pairs_in_cell(stars.assign(cell=0, is_home=True))
v = geometry.unit_vectors(stars["ra"].to_numpy(), stars["dec"].to_numpy())
max_chord = 2 * np.sin(np.radians(config.S_MAX_AU * 10 / 1000 / 3600) / 2)
kd = {tuple(sorted((stars["source_id"][i], stars["source_id"][j]))) for i, j in cKDTree(v).query_pairs(max_chord)}

found = set(zip(by_cells["source_id_1"], by_cells["source_id_2"]))
expected = set(zip(brute["source_id_1"], brute["source_id_2"]))
print(f"пар через комірки {len(by_cells)}, унікальних {len(found)}, перебором {len(expected)}")
assert len(found) == len(by_cells), "є дублікати пар"
assert found == expected, f"розбіжність: лише в комірках {len(found - expected)}, лише в переборі {len(expected - found)}"
assert expected <= kd, "перебір знайшов пару, якої немає серед кандидатів cKDTree"
recovered = len(injected & found) / len(injected)
print(f"підкладених пар знайдено: {recovered:.1%}")
assert recovered > 0.95

# ---- 3. copies into all 8 neighbours give the same pairs after dropDuplicates
copies_all = pd.concat(geometry.add_copies([stars], all_neighbours=True), ignore_index=True)
by_cells_all = pd.concat([geometry.pairs_in_cell(g) for _, g in copies_all.groupby("cell")], ignore_index=True)
found_all = set(zip(by_cells_all["source_id_1"], by_cells_all["source_id_2"]))
print(f"усі сусіди: рядків {len(copies_all)} ({len(copies_all) / n:.2f} на зірку, очікуємо ≈9), "
      f"пар до dropDuplicates {len(by_cells_all)}, після {len(found_all)}")
assert found_all == found, "варіант з усіма сусідами знайшов інші пари"
print("OK")
