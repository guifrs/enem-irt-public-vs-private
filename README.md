# Public vs. Private High-School Effect on ENEM 2017 Scores  
_Item Response Theory (IRT) approach_

[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

This repository reproduces every step of my undergraduate thesis, which explores how attending a public high school affects students’ scores on the 2017 ENEM (Brazil’s national exam). Because ENEM is scored with Item Response Theory (IRT), candidates with the same number of correct answers may receive different grades; this characteristic is factored into all analyses.

The analysis relies on **multiple linear regression models** to isolate the effect of public school attendance on ENEM scores, while holding constant the number of correct answers, socioeconomic status, and demographic characteristics.

**Author:** Guilherme Ferreira Ribeiro dos Santos  
**Institution:** School of Economics, Business and Accounting — University of São Paulo (FEA-USP)  
**Year:** 2024  

---

## Repository structure
```text
enem-irt-public-vs-private/
│
├── README.md
├── LICENSE
├── .gitignore
├── pyproject.toml
├── uv.lock 
│
├── data/
│   ├── raw/                    # original micro-data (✗ not versioned)
│   └── processed/              # cleaned Parquet files
│
├── notebooks/
│   ├── 01_download_and_clean.ipynb
│   ├── 02_exploratory_analysis.ipynb
│   ├── 03_regression_models.ipynb
│   └── 04_figures_and_tables.ipynb
│
├── src/
│   ├── __init__.py
│   ├── data/
│   │   └── make_dataset.py
│   ├── features/
│   │   └── build_features.py
│   ├── models/
│   │   └── linear_models.py
│   └── visualization/
│       └── plots.py
│
├── tests/
│   └── test_make_dataset.py
│
├── docs/
│   └── thesis_PT_BR.pdf
└── results/
    ├── figures/
    └── tables/
```

## Quick start
```bash
# clone the repo
git clone https://github.com/guifrs/enem-irt-public-vs-private.git
cd enem-irt-public-vs-private

# (one-time) install uv globally if you don’t have it yet
python -m pip install --upgrade uv        # or: brew install uv

# create and activate an isolated virtual environment with uv
uv venv .venv
source .venv/bin/activate                 # Windows: .venv\Scripts\activate

# install project dependencies — fast!
uv sync
```
Run the notebooks in numerical order (01_, 02_, …) to fully reproduce the analysis.

## Data
The raw ENEM 2017 micro-data (~4 GB) are not stored in this repository due to size and licensing.
```src/data/make_dataset.py``` automatically downloads and extracts the files directly from INEP’s official link.

## Abstract
<details> <summary>Click to read the full abstract</summary>
This study analyzed the impact of having attended public high school on students’ performance
in the 2017 ENEM (National High School Exam), considering the influence of Item Response
Theory (IRT) on the scores. Multiple linear regression models were used to evaluate the effect of
school origin on the scores, controlling for the number of correct answers and socioeconomic
and demographic variables.

The results indicated that, in Languages and Codes, public-school students obtained slightly higher
scores when answering the same number of questions correctly, although this effect becomes
irrelevant after additional controls. In Humanities, Natural Sciences, and Mathematics, public-school
students presented lower scores, even with the same number of correct answers, with the
effect being more pronounced in Mathematics.

The interaction between Public School and Number of Correct Answers revealed that the impact of
correct answers on the score is greater for public-school students, suggesting diminishing
marginal returns.

We conclude that school origin exerts a significant influence on the educational inequalities
evidenced in the ENEM, highlighting the importance of public policies aimed at reducing these
disparities.

</details>

## License
This project is licensed under the MIT License – see the LICENSE file for details.

## How to cite
```bibtex
@thesis{Santos2024,
  title  = {Public vs. Private High-School Effect on ENEM 2017 Scores: An Item Response Theory Analysis},
  author = {Santos, Guilherme Ferreira Ribeiro dos},
  school = {University of São Paulo},
  year   = {2024}
}
```
Made with ❤️ and Python.
