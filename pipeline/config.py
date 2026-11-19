"""Task parameters and paths inside the container."""
from pathlib import Path

from pyspark.sql import SparkSession

# ---- paths ------------------------------------------------------------------
OUTPUT = Path("/work/output")
STARS = OUTPUT / "stage1_stars.parquet"
PAIRS = OUTPUT / "stage2_pairs.parquet"
PAIRS_CLEAN = OUTPUT / "stage3_pairs_clean.parquet"
SYSTEMS = OUTPUT / "stage4_systems.parquet"
PAIRS_SHIFTED = OUTPUT / "stage5_pairs_shifted.parquet"
STATS = OUTPUT / "stage6_stats.csv"
PLANS = OUTPUT / "plans"                       # explain("formatted") of every stage
METRICS = OUTPUT / "metrics.json"              # metrics of the last run
RUNS = OUTPUT / "runs"                         # metrics of every run: runs/<app_id>.json

# ---- stage 7: El-Badry et al. (2021) catalog --------------------------------
ELBADRY_FITS = Path("/data/elbadry/all_columns_catalog.fits.gz")
ELBADRY = OUTPUT / "elbadry_catalog.parquet"
MANIFEST = Path("/work/dataset/manifest.json")  # HEALPix pixel ranges of the input files
VALIDATION = OUTPUT / "stage7_validation.csv"
R_HIGH_CONFIDENCE = 0.1                        # R < 0.1: high-confidence pairs in their catalog

# ---- HEALPix ----------------------------------------------------------------
NSIDE = 256                                    # level 8: cell ≈ 0.23° = 824″ > θ_max = 500″
CELL_SHIFT = 43                                # source_id >> 43 = level-8 cell index (NESTED)

# ---- star filter (stage 1) --------------------------------------------------
PARALLAX_MIN = 1.0                             # mas: no farther than 1000 pc
PARALLAX_MAX = 10.0                            # mas: no closer than 100 pc, keeps θ_max ≤ 500″
PARALLAX_OVER_ERROR_MIN = 5.0                  # distance error < 20%; implies σ_ϖ < 2 mas for ϖ ≤ 10

# ---- pair criteria (stage 2), ϖ1 is the parallax of the brighter star -------
S_MAX_AU = 50_000                              # maximum projected separation, AU
PARALLAX_N_SIGMA = 3                           # |ϖ1 − ϖ2| < b·√(σ1² + σ2²), b = 3 ...
PARALLAX_N_SIGMA_CLOSE = 6                     # ... or b = 6 for θ < 4″
CLOSE_THETA_ARCSEC = 4.0
DMU_ORBIT_COEF = 0.44428                       # Δμ ≤ 0.44428·ϖ1^1.5·θ^-0.5 + 2σ_Δμ (M ≈ 5 M☉)
DMU_N_SIGMA = 2

# ---- stages 3-6 -------------------------------------------------------------
MAX_COMPANIONS = 3                             # more companions means the star is in a cluster
MAX_ITERATIONS = 20                            # upper limit for label propagation
SHIFT_DEG = 0.5                                # dec shift of the catalog copy
THETA_MIN_ARCSEC = 0.5                         # Gaia resolves closer stars poorly
CHANCE_WEIGHT = 0.5                            # original × copy gives N² combinations, real pairs N²/2

COLUMNS = [
    "source_id", "ra", "dec",
    "parallax", "parallax_error",
    "pmra", "pmra_error", "pmdec", "pmdec_error",
    "phot_g_mean_mag", "bp_rp", "ruwe",
]


def spark(app_name: str) -> SparkSession:
    """Create or return the SparkSession with the given application name.

    Master, memory and the event log are set in conf/spark-defaults.conf,
    so nothing but the name is configured here.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()
