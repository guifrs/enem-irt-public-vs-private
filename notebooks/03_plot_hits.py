from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# --------------------------------------------------------------------
# Logging configuration (with timestamp)
# --------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROC_DIR = PROJECT_ROOT / "data" / "processed" / "microdados_enem_2017"
HITS_PARQUET = PROC_DIR / "enem_2017_hits.parquet"

FIG_DIR = PROJECT_ROOT / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)
FIG_PATH = FIG_DIR / "hits_by_exam_enem2017.png"


# --------------------------------------------------------------------
# Plot function
# --------------------------------------------------------------------


def plot_hits(
    parquet_path: Path = HITS_PARQUET,
    save_path: Path = FIG_PATH,
) -> Path:
    """
    Load the hits parquet and generate scatter plots showing the
    relationship between number of correct answers and scores for
    each ENEM exam (science, humanities, language, math).

    The figure is saved to `save_path` and optionally displayed.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Hits parquet not found: {parquet_path}")

    logging.info("Loading dataset: %s", parquet_path)
    df = pd.read_parquet(parquet_path)

    exams = ["science", "humanities", "language", "math"]
    labels = {
        "science": "Natural Sciences",
        "humanities": "Humanities",
        "language": "Languages and Codes",
        "math": "Mathematics",
    }

    fig, axs = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        "Relationship between Correct Answers and Scores by Exam – ENEM 2017",
        fontsize=16,
        fontweight="bold",
    )

    for ax, exam in zip(axs.flatten(), exams):
        col_hits = f"hits_{exam}"
        col_score = f"score_{exam}"
        col_above = f"above_median_{exam}"

        # Above / below median
        above = df[df[col_above] == 1]
        below = df[df[col_above] == 0]

        # Scatter plots
        ax.scatter(
            above[col_hits],
            above[col_score],
            color="blue",
            alpha=0.5,
            edgecolors="w",
            linewidth=0.5,
            label="Score > Median",
        )
        ax.scatter(
            below[col_hits],
            below[col_score],
            color="red",
            alpha=0.5,
            edgecolors="w",
            linewidth=0.5,
            label="Score ≤ Median",
        )

        # Median by number of hits
        median_plot = df.groupby(col_hits)[col_score].median()

        ax.scatter(
            median_plot.index,
            median_plot.values,
            color="black",
            marker="o",
            label="Median",
        )

        # Titles and labels (English)
        exam_label = labels[exam]
        ax.set_title(
            f"Relationship between Correct Answers and Scores in {exam_label}",
            fontsize=12,
        )
        ax.set_xlabel(f"Number of Correct Answers in {exam_label}", fontsize=10)
        ax.set_ylabel(f"Score in {exam_label}", fontsize=10)

        ax.grid(True, linestyle="--", alpha=0.7)

        # Handle Decimal -> int for ticks
        max_hits = int(median_plot.index.max())
        max_score = int(df[col_score].max())

        ax.set_xticks(range(0, max_hits + 1, 3))
        ax.set_yticks(range(0, max_score + 101, 100))

        ax.legend(loc="upper left")

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    # Save figure
    plt.savefig(save_path, dpi=100, bbox_inches="tight")
    logging.info("Figure saved to %s", save_path)

    return save_path


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

if __name__ == "__main__":
    plot_hits()
