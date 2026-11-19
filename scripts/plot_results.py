"""Figures from the stage 6 and 7 results. Run in the container (it has matplotlib):

    podman compose run --rm spark python3 scripts/plot_results.py

Input:  output/stage6_stats.csv, output/stage7_validation.csv (if present)
Output: output/figures/separation.png, output/figures/validation.png
(the spark user in the container cannot write to docs/, so copy from the host:
 mkdir -p docs/figures && cp output/figures/*.png docs/figures/)
Counts and fractions have different scales, so they go into separate panels instead of two Y axes.
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "output"
FIGURES = OUTPUT / "figures"

# palette (categorical slots 1 and 2, checked for colour blindness) and neutral styling
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e1e0d9", "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
    "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "legend.frameon": False,
})


def bin_labels(bins):
    """Return separation bin labels like "100-316" (bounds in AU).

    Bin b covers s from 10^(b/2) to 10^((b+1)/2), as on stage 6.
    """
    def fmt(x):
        return f"{x:,.0f}".replace(",", " ")
    return [f"{fmt(10 ** (b / 2))}-{fmt(10 ** ((b + 1) / 2))}" for b in bins]


def comma(x, _=None):
    """matplotlib axis formatter: a number with a decimal comma."""
    return f"{x:g}".replace(".", ",")


def separation():
    """Draw separation.png from stage6_stats.csv.

    Left: found pairs and chance alignments per bin (log scale);
    right: the chance alignment fraction in percent.
    """
    t = pd.read_csv(OUTPUT / "stage6_stats.csv").sort_values("bin")
    x = range(len(t))
    labels = bin_labels(t["bin"])
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4), gridspec_kw={"width_ratios": [3, 2]})

    w = 0.38
    a.bar([i - w / 2 - 0.01 for i in x], t["real"], w, color=BLUE, label="знайдені пари (етап 3)")
    a.bar([i + w / 2 + 0.01 for i in x], t["chance"].clip(lower=0.3), w, color=ORANGE,
          label="випадкові збіги (етап 5, вага 1/2)")
    a.set_yscale("log")
    a.set_title("Пари за проєкційною відстанню s")
    a.set_xlabel("s, а.о.")
    a.set_ylabel("кількість пар")
    a.set_xticks(list(x), labels, rotation=30, ha="right")
    a.legend(loc="upper right")
    a.yaxis.set_major_formatter(lambda v, _: f"{v:,.0f}".replace(",", " "))

    frac = 100 * t["chance_fraction"]
    b.plot(list(x), frac, color=ORANGE, linewidth=2, marker="o", markersize=6)
    for i, v in zip(x, frac):
        if v >= 0.5:  # label only noticeable values, not every point
            b.annotate(f"{v:.1f}%".replace(".", ","), (i, v), textcoords="offset points",
                       xytext=(-6, 8), ha="right", color=INK_2)
    b.set_title("Частка випадкових збігів")
    b.set_xlabel("s, а.о.")
    b.set_ylabel("випадкові / знайдені, %")
    b.set_xticks(list(x), labels, rotation=30, ha="right")
    b.set_ylim(0, max(frac) * 1.2)
    b.yaxis.set_major_formatter(comma)

    fig.tight_layout()
    fig.savefig(FIGURES / "separation.png", dpi=150)
    print(f"-> {FIGURES / 'separation.png'}")


def validation():
    """Draw validation.png from stage7_validation.csv: completeness and purity per s bin.

    If stage 7 has not been run and the file is missing, the figure is skipped.
    """
    path = OUTPUT / "stage7_validation.csv"
    if not path.exists():
        print("stage7_validation.csv немає, рисунок валідації пропущено")
        return
    t = pd.read_csv(path).sort_values("bin")
    t = t[t["theirs"] > 0]
    x = list(range(len(t)))
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(x, 100 * t["completeness_stage3"], color=BLUE, linewidth=2, marker="o", markersize=6,
            label="повнота: частка їхніх пар, знайдених у нас")
    ax.plot(x, 100 * t["purity_stage3"], color=ORANGE, linewidth=2, marker="s", markersize=6,
            label="чистота: частка наших пар, що є в їхньому каталозі")
    ax.set_title("Порівняння з каталогом El-Badry et al. (2021), етап 3")
    ax.set_xlabel("s, а.о.")
    ax.set_ylabel("%")
    ax.set_xticks(x, bin_labels(t["bin"]), rotation=30, ha="right")
    ax.set_ylim(0, 105)
    ax.legend(loc="lower left")
    fig.tight_layout()
    fig.savefig(FIGURES / "validation.png", dpi=150)
    print(f"-> {FIGURES / 'validation.png'}")


if __name__ == "__main__":
    FIGURES.mkdir(parents=True, exist_ok=True)
    separation()
    validation()
