"""Figures from the stage 6 and 7 results. Run in the container (it has matplotlib):

    podman compose run --rm spark python3 scripts/plot_results.py               # full run in evidence/
    podman compose run --rm spark python3 scripts/plot_results.py --data output # last run

Input:  <data>/stage6_stats.csv, <data>/stage7_validation.csv (if present), <data> = evidence by default
Output: output/figures/separation.png, output/figures/validation.png (committed, unlike the rest of output/)
Counts and fractions have different scales, so they go into separate panels instead of two Y axes.
"""
import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "evidence"
FIGURES = ROOT / "output" / "figures"

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
    t = pd.read_csv(DATA / "stage6_stats.csv").sort_values("bin")
    x = range(len(t))
    labels = bin_labels(t["bin"])
    fig, (a, b) = plt.subplots(1, 2, figsize=(11, 4), gridspec_kw={"width_ratios": [3, 2]})

    w = 0.38
    a.bar([i - w / 2 - 0.01 for i in x], t["real"], w, color=BLUE, label="знайдені пари (етап 3)")
    chance_x = [i + w / 2 + 0.01 for i in x]
    a.bar(chance_x, t["chance"].where(t["chance"] > 0), w, color=ORANGE,
          label="випадкові збіги (етап 5, вага 1/2)")
    for cx, c in zip(chance_x, t["chance"]):
        if c == 0:  # a zero has no bar on a log scale, so mark it with text
            a.annotate("0", (cx, 0.3), textcoords="offset points", xytext=(0, 2), ha="center", color=INK_2)
    a.set_yscale("log")
    a.set_ylim(0.3, t["real"].max() * 30)  # headroom for the legend above the tallest bar
    a.set_title("Пари за проєкційною відстанню s")
    a.set_xlabel("s, а.о.")
    a.set_ylabel("кількість пар")
    a.set_xticks(list(x), labels, rotation=30, ha="right")
    a.legend(loc="upper left", ncols=2)
    a.yaxis.set_major_formatter(lambda v, _: f"{v:,.0f}".replace(",", " "))

    frac = 100 * t["chance_fraction"]
    b.plot(list(x), frac, color=ORANGE, linewidth=2, marker="o", markersize=6)
    last = len(frac) - 1
    for i, v in zip(x, frac):
        if v >= 0.5:  # label only noticeable values, not every point
            # the line rises to the right, so labels sit up-left of the point, the last one above it
            offset, align = ((0, 9), "center") if i == last else ((-6, 5), "right")
            b.annotate(f"{v:.1f}%".replace(".", ","), (i, v), textcoords="offset points",
                       xytext=offset, ha=align, color=INK_2)
    b.set_title("Частка випадкових збігів")
    b.set_xlabel("s, а.о.")
    b.set_ylabel("випадкові / знайдені, %")
    b.set_xticks(list(x), labels, rotation=30, ha="right")
    b.set_ylim(0, max(frac) * 1.25)
    b.yaxis.set_major_formatter(comma)

    fig.tight_layout()
    fig.savefig(FIGURES / "separation.png", dpi=150)
    print(f"-> {FIGURES / 'separation.png'}")


def validation():
    """Draw validation.png from stage7_validation.csv: completeness and purity per s bin.

    If stage 7 has not been run and the file is missing, the figure is skipped.
    """
    path = DATA / "stage7_validation.csv"
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
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", type=Path, default=DATA, help="folder with the stage 6 and 7 CSV files")
    DATA = ap.parse_args().data.resolve()
    FIGURES.mkdir(parents=True, exist_ok=True)
    separation()
    validation()
