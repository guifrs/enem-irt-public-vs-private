# Causal Inference in ENEM 2017

_Exploring how public vs. private high schools shape student performance under Item Response Theory_

[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

This project investigates whether attending a public high school causally affects ENEM 2017 performance, controlling for ability, socioeconomic status, demographics, and exam characteristics.

ENEM uses Item Response Theory (IRT) — meaning two students with the same number of correct answers can receive different final scores depending on the pattern of responses.

This unique scoring rule makes ENEM a rich environment for applied causal inference.

## 🎯 Motivation

While exploring the microdata, I plotted the relationship between number of correct answers and final IRT-adjusted score using the script `03_plot_hits.py`.

![alt text](image-1.png)

One surprising pattern emerged:

> Students with the same number of correct answers often receive drastically different scores — sometimes 150+ points apart.

This opened two key questions:

**1. What explains this score variation?**

    If students guess or show inconsistent patterns across items, IRT penalizes them.


**2. Is there a systematic disadvantage for public-school students?**

  A hypothesis emerges:

>    Students from public schools may have experienced larger learning gaps during high school, producing answer patterns that IRT interprets as lower “ability”, even when achieving the same number of correct answers.

This repository explores this hypothesis rigorously.

## 📊 Regression Results — What We Learn

One of the core outputs is the regression table for Mathematics.
Below is a simplified version of the model progression:

|                         | (1)                 | (2)                 | (3)                 | (4)                 | (5)                 |
|-------------------------|---------------------|---------------------|---------------------|---------------------|---------------------|
| **Constant**            | 589.00*** (0.10)    | 305.13*** (0.09)    | 305.21*** (0.10)    | 293.08*** (0.15)    | 300.75*** (0.22)    |
| **Public School**       | -89.08*** (0.11)    | -14.14*** (0.06)    | -14.16*** (0.06)    | -7.99*** (0.06)     | -7.93*** (0.06)     |
| **Number of Correct Answers** |                     | 19.06*** (0.01)    | 19.05*** (0.01)    | 18.67*** (0.01)     | 18.51*** (0.01)     |
| **Exam Code Controls**  |                     |                     | Yes                 | Yes                 | Yes                 |
| **Income Controls**     |                     |                     |                     | Yes                 | Yes                 |
| **Sex Control**         |                     |                     |                     |                     | Yes                 |
| **Race Controls**       |                     |                     |                     |                     | Yes                 |
| **R²**                  | 0.12                | 0.79                | 0.79                | 0.80                | 0.80                |
| **N**                   | 4,423,760           | 4,423,760           | 4,423,760           | 4,423,760           | 4,423,760           |


### 🔎 Interpretation

**Model (1): Raw difference**

Public-school students score 89 points lower, on average.
But this is a naïve comparison: it ignores differences in ability, income, or background.

**Model (2): Controlling for number of correct answers**

Once we adjust for the actual knowledge demonstrated, the gap shrinks to 14 points.
This means:

> Even when answering the same number of items correctly, public-school students still receive lower IRT scores.

**Models (3)–(5): Adding exam code, income, sex, and race controls**

After fully controlling for background and exam characteristics, the gap drops to ~8 points.
This residual difference is consistent with the hypothesis:

> Public-school students may produce answer patterns that IRT interprets as lower latent ability (guessing or inconsistent response behavior).

R² ≈ 0.80
Models explain 80% of variation — unusually high for microdata, thanks to the strong predictive power of number of correct answers.

## 🛠️ Technologies & Performance Considerations

Throughout the project, I intentionally used different libraries to understand their performance and ergonomics when handling millions of rows:

### Polars

Used for downloading, cleaning, and converting the raw CSV (~4 GB) into an optimized Parquet file.

→ Extremely fast and memory-efficient.

### DuckDB

Used in `02_build_hits.py` to generate item-level expansions and compute the number of correct answers.

→ Perfect for large aggregations without loading everything into RAM.

### Pandas & Matplotlib

Used for analysis and visualization (`03_plot_hits.py`).

→ Best suited for plotting workflows and exploratory analysis.

### Statsmodels

Used for all regression models (`04_regressions.py`).

→ Clear API for OLS, easy to export results to tables.

This modular pipeline mirrors real data-engineering workflows and showcases how each tool excels in its niche.

## 📁 Project Structure

```
enem-irt-public-vs-private/
│
├── data/
│   ├── raw/                      # Downloaded microdata (not versioned)
│   └── processed/                # Cleaned Parquet + hits dataset
│
├── notebooks/
│   ├── 01_download_and_clean.py  # Polars ETL
│   ├── 02_build_hits.py          # DuckDB item expansions
│   ├── 03_plot_hits.py           # Exploratory plots
│   └── 04_regressions.py         # Statsmodels regressions
│
├── figures/                      # All generated plots
├── tables/regressions/           # Regression tables (CSV)
│
├── thesis_PT_BR.pdf              # Full thesis, in Portuguese
├── pyproject.toml
├── uv.lock
├── LICENSE
└── README.md
```

## 🚀 How to Run the Project

1. Clone the repository

```
git clone https://github.com/guifrs/enem-irt-public-vs-private.git
cd enem-irt-public-vs-private
```

2. Install dependencies using uv

```
uv venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
uv sync
```

3. Run the pipeline scripts in order

```
uv run notebooks/01_download_and_clean.py
uv run notebooks/02_build_hits.py
uv run notebooks/03_plot_hits.py
uv run notebooks/04_regressions.py
```

## 📄 Full Thesis (Portuguese)

The full academic version of this study — including additional models, theoretical background, and robustness checks — is available in the repository:

➡️ thesis_PT_BR.pdf

## 📬 Contact

Feel free to open issues or reach out for collaboration, suggestions, or research discussions.