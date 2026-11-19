"""Sky geometry and the pair search inside a cell. numpy and healpy only, no Spark.

Spark calls these functions inside mapInPandas and applyInPandas: each takes
a pandas DataFrame and returns a pandas DataFrame.
"""
import healpy as hp
import numpy as np
import pandas as pd

from pipeline import config

ARCSEC_PER_RAD = 180 / np.pi * 3600

STAR_SCHEMA = (
    "source_id long, ra double, dec double, parallax double, parallax_error double, "
    "pmra double, pmra_error double, pmdec double, pmdec_error double, "
    "phot_g_mean_mag double, bp_rp double, ruwe double, cell long, origin int"
)
COPY_SCHEMA = STAR_SCHEMA + ", is_home boolean"
PAIR_SCHEMA = (
    "source_id_1 long, source_id_2 long, cell long, theta_arcsec double, s_au double, "
    "parallax_1 double, parallax_2 double, dmu double, dmu_limit double"
)


def unit_vectors(ra_deg, dec_deg):
    """Convert equatorial coordinates in degrees to unit vectors.

    Returns an (n, 3) array of Cartesian coordinates of the points on the sphere.
    """
    ra, dec = np.radians(ra_deg), np.radians(dec_deg)
    return np.column_stack([np.cos(dec) * np.cos(ra), np.cos(dec) * np.sin(ra), np.sin(dec)])


def angle_arcsec(v1, v2):
    """Return the angle between pairs of unit vectors in arcseconds.

    The angle is computed from the chord length, because arccos of the dot
    product loses precision at small angles.
    """
    chord = np.linalg.norm(v1 - v2, axis=1)
    return 2 * np.arcsin(np.clip(chord / 2, 0, 1)) * ARCSEC_PER_RAD   # θ = 2·arcsin(c/2)


def cell_of(ra_deg, dec_deg):
    """Return the level-8 HEALPix cell index (NESTED scheme) for the given coordinates."""
    return hp.ang2pix(config.NSIDE, ra_deg, dec_deg, nest=True, lonlat=True).astype(np.int64)


def add_copies(batches, all_neighbours=False):
    """For mapInPandas: add copies of every star to neighbouring cells.

    A star stays in its own cell (is_home=True) and is copied into the neighbouring
    cells with a smaller index than its own (is_home=False). Then a pair of stars in
    cells c_a < c_b is found only in c_a, and every pair is counted exactly once.
    On average there are 4 copies, so the row count becomes ≈5N. With all_neighbours=True
    copies go to all 8 neighbours (≈9N rows) and a pair across cells is found twice.
    """
    for pdf in batches:
        cells = pdf["cell"].to_numpy()
        out = [pdf.assign(is_home=True)]
        neighbours = hp.get_all_neighbours(config.NSIDE, cells, nest=True)   # (8, n), -1 = no neighbour
        for nb in neighbours:
            mask = (nb >= 0) if all_neighbours else (nb >= 0) & (nb < cells)
            if mask.any():
                out.append(pdf[mask].assign(cell=nb[mask], is_home=False))
        yield pd.concat(out, ignore_index=True)


def shift_catalog(batches):
    """For mapInPandas: shift stars by SHIFT_DEG in dec and recompute the cell.

    Shifted stars get origin = 1. Stars pushed past the pole are dropped.
    """
    for pdf in batches:
        dec = pdf["dec"] + config.SHIFT_DEG
        keep = dec <= 90
        shifted = pdf[keep].assign(dec=dec[keep], origin=1)
        shifted["cell"] = cell_of(shifted["ra"].to_numpy(), shifted["dec"].to_numpy())
        yield shifted


def candidate_indices(n_home, n_all):
    """Return the indices (i, j) of the cell rows that have to be compared.

    Rows 0..n_home−1 are home stars, the rest are copies from neighbouring cells.
    Home stars are compared with each other (i < j) and every home star with every copy.
    """
    i1, j1 = np.triu_indices(n_home, k=1)
    i2, j2 = np.meshgrid(np.arange(n_home), np.arange(n_home, n_all), indexing="ij")
    return np.concatenate([i1, i2.ravel()]), np.concatenate([j1, j2.ravel()])


def pairs_in_cell(pdf):
    """For applyInPandas on stage 2: return all pairs in the cell that pass the criteria."""
    return _pairs(pdf, cross_only=False)


def chance_pairs_in_cell(pdf):
    """For applyInPandas on stage 5: return only original-shifted copy pairs.

    By construction none of them is a real pair, so they are chance alignments.
    """
    return _pairs(pdf, cross_only=True)


def _pairs(pdf, cross_only):
    pdf = pdf.sort_values("is_home", ascending=False, kind="stable").reset_index(drop=True)
    n_home, n_all = int(pdf["is_home"].sum()), len(pdf)
    if n_home == 0 or n_all < 2:
        return empty_pairs()

    i, j = candidate_indices(n_home, n_all)
    if cross_only:
        cross = pdf["origin"].to_numpy()[i] != pdf["origin"].to_numpy()[j]
        i, j = i[cross], j[cross]

    a, b = pdf.iloc[i].reset_index(drop=True), pdf.iloc[j].reset_index(drop=True)
    v = unit_vectors(pdf["ra"].to_numpy(), pdf["dec"].to_numpy())
    theta = angle_arcsec(v[i], v[j])

    # ϖ1 is the parallax of the brighter star (smaller G magnitude)
    a_brighter = (a["phot_g_mean_mag"] <= b["phot_g_mean_mag"]).to_numpy()
    plx_1 = np.where(a_brighter, a["parallax"], b["parallax"])

    # 1) θ < s_max · ϖ1 / 1000
    ok = theta < config.S_MAX_AU * plx_1 / 1000

    # 2) |ϖ1 − ϖ2| < b·√(σ1² + σ2²), b = 3, or b = 6 for θ < 4″
    d_plx = np.abs(a["parallax"] - b["parallax"]).to_numpy()
    sig_plx = np.hypot(a["parallax_error"], b["parallax_error"]).to_numpy()
    n_sigma = np.where(theta < config.CLOSE_THETA_ARCSEC,
                       config.PARALLAX_N_SIGMA_CLOSE, config.PARALLAX_N_SIGMA)
    ok &= d_plx < n_sigma * sig_plx

    # 3) Δμ ≤ 0,44428·ϖ1^1.5·θ^-0.5 + 2σ_Δμ,
    #    σ_Δμ² = (Δμ_α²·(σ²_μα1 + σ²_μα2) + Δμ_δ²·(σ²_μδ1 + σ²_μδ2)) / Δμ²
    d_ra = (a["pmra"] - b["pmra"]).to_numpy()
    d_dec = (a["pmdec"] - b["pmdec"]).to_numpy()
    dmu = np.hypot(d_ra, d_dec)
    var_ra = (a["pmra_error"] ** 2 + b["pmra_error"] ** 2).to_numpy()
    var_dec = (a["pmdec_error"] ** 2 + b["pmdec_error"] ** 2).to_numpy()
    sig_dmu = np.sqrt((d_ra ** 2 * var_ra + d_dec ** 2 * var_dec) / np.maximum(dmu, 1e-12) ** 2)
    dmu_limit = (config.DMU_ORBIT_COEF * plx_1 ** 1.5 / np.sqrt(np.maximum(theta, 1e-3))
                 + config.DMU_N_SIGMA * sig_dmu)
    ok &= dmu <= dmu_limit

    a, b = a[ok], b[ok]
    id_a, id_b = a["source_id"].to_numpy(), b["source_id"].to_numpy()
    swap = id_a > id_b
    return pd.DataFrame({
        "source_id_1": np.where(swap, id_b, id_a),
        "source_id_2": np.where(swap, id_a, id_b),
        "cell": pdf["cell"].iloc[0],
        "theta_arcsec": theta[ok],
        "s_au": theta[ok] * 1000 / plx_1[ok],          # s[AU] = θ[″] / ϖ1[″] = θ·1000 / ϖ1[mas]
        "parallax_1": np.where(swap, b["parallax"], a["parallax"]),
        "parallax_2": np.where(swap, a["parallax"], b["parallax"]),
        "dmu": dmu[ok],
        "dmu_limit": dmu_limit[ok],
    })


def empty_pairs():
    """Return an empty pair table with the PAIR_SCHEMA columns and types.

    Types are set explicitly (int64, float64): with the object dtype pandas warns
    on concat and Arrow cannot infer the column type.
    """
    types = {"long": "int64", "double": "float64"}
    return pd.DataFrame({c.split()[0]: pd.Series(dtype=types[c.split()[1]]) for c in PAIR_SCHEMA.split(", ")})
