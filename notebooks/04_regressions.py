from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import statsmodels.api as sm

# --------------------------------------------------------------------
# Logging configuration (with timestamp)
# --------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --------------------------------------------------------------------
# Paths & constants
# --------------------------------------------------------------------

# repo_root/notebooks/04_regressions.py -> parents[1] = repo_root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROC_DIR = PROJECT_ROOT / "data" / "processed"
HITS_PARQUET = PROC_DIR / "microdados_enem_2017" / "enem_2017_hits.parquet"

REG_TABLE_DIR = PROJECT_ROOT / "tables" / "regressions"
REG_TABLE_DIR.mkdir(parents=True, exist_ok=True)

VALID_EXAM_CODES = [
    392,
    393,
    391,
    394,
    395,
    396,
    397,
    398,
    399,
    400,
    401,
    402,
    403,
    404,
    405,
    406,
]

# mapping from ENEM area code to suffix used in enem_2017_hits.parquet
AREAS = {
    "CN": "science",
    "CH": "humanities",
    "LC": "language",
    "MT": "math",
}


# --------------------------------------------------------------------
# Data preparation
# --------------------------------------------------------------------


def load_base_df(parquet_path: Path = HITS_PARQUET) -> pd.DataFrame:
    """
    Load enem_2017_hits parquet and create base features that are common
    to all exam areas (public_school, is_female, is_black, low_income).
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Hits parquet not found: {parquet_path}")

    logging.info("Loading hits dataset from %s", parquet_path)
    df = pd.read_parquet(parquet_path)
    logging.info("Raw hits shape: %s", df.shape)

    # rename to shorter names
    df = df.rename(
        columns={
            "family_income_bracket": "income_bracket",
            "school_funding_src": "school_funding",
        }
    )

    # Main features (mirror of your notebook logic)
    # NOTE: we assume:
    #   - school_funding == 1  -> public school
    #   - sex == 1             -> female
    #   - race_color == 2      -> black
    #   - income_bracket in {1, 2} -> low income
    df["public_school"] = (df["school_funding"] == 1).astype(np.int8)
    df["is_female"] = (df["sex"] == 1).astype(np.int8)
    df["is_black"] = (df["race_color"] == 2).astype(np.int8)
    df["low_income"] = df["income_bracket"].isin([1, 2]).astype(np.int8)

    return df


def prepare_area_df(df: pd.DataFrame, area: str) -> pd.DataFrame:
    """
    Given the wide hits dataframe and an area code (CN, CH, LC, MT),
    return a smaller dataframe for that area only with generic column
    names: exam_code, presence, score, hits, etc.
    """
    suffix = AREAS[area]

    col_exam = f"code_exam_{suffix}"
    col_presence = f"presence_{suffix}"
    col_score = f"score_{suffix}"
    col_hits = f"hits_{suffix}"

    needed = [
        "registration_id",
        "exam_year",
        "income_bracket",
        "school_funding",
        "sex",
        "race_color",
        "public_school",
        "is_female",
        "is_black",
        "low_income",
        col_exam,
        col_presence,
        col_score,
        col_hits,
    ]

    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns for area {area}: {missing}")

    area_df = df[needed].copy()

    # keep only present students
    area_df = area_df[area_df[col_presence] == 1].copy()

    # keep valid exam codes
    area_df = area_df[area_df[col_exam].isin(VALID_EXAM_CODES)].copy()

    # drop rows with missing core info
    area_df = area_df.dropna(subset=[col_exam, col_score, col_hits, "income_bracket"])

    # rename to generic names
    area_df = area_df.rename(
        columns={
            col_exam: "exam_code",
            col_presence: "presence",
            col_score: "score",
            col_hits: "hits",
        }
    )

    # enforce types
    area_df["exam_code"] = area_df["exam_code"].astype(int)
    area_df["income_bracket"] = area_df["income_bracket"].astype(int)
    area_df["race_color"] = area_df["race_color"].astype(int)

    logging.info("Prepared area %s dataframe with shape %s", area, area_df.shape)
    return area_df


# --------------------------------------------------------------------
# Regression helpers
# --------------------------------------------------------------------


def _run_ols(y: pd.Series, X: pd.DataFrame):
    """
    Internal helper to run OLS with a constant.

    - Concatenates y and X
    - Forces all columns to numeric (float), coercing errors to NaN
    - Drops rows with NaN
    - Fits OLS
    """
    # Put everything together
    data = pd.concat([y, X], axis=1)

    # Force numeric types (float); anything não-numérico vira NaN
    for col in data.columns:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Drop rows with NaN in any column
    data = data.dropna()

    # First column is y, rest are X
    y_clean = data.iloc[:, 0].astype(float)
    X_clean = data.iloc[:, 1:].astype(float)

    # Add constant and fit
    X_const = sm.add_constant(X_clean, has_constant="add")
    model = sm.OLS(y_clean, X_const).fit()

    return model


def run_model_1(area_df: pd.DataFrame):
    """Model 1: score ~ public_school"""
    y = area_df["score"]
    X = area_df[["public_school"]]
    return _run_ols(y, X)


def run_model_2(area_df: pd.DataFrame):
    """Model 2: score ~ public_school + hits"""
    y = area_df["score"]
    X = area_df[["public_school", "hits"]]
    return _run_ols(y, X)


def run_model_3(area_df: pd.DataFrame):
    """Model 3: score ~ public_school + hits + exam_code dummies"""
    df = pd.get_dummies(area_df, columns=["exam_code"], drop_first=True, dtype=int)
    y = df["score"]
    X = df[
        ["public_school", "hits"]
        + [c for c in df.columns if c.startswith("exam_code_")]
    ]
    return _run_ols(y, X)


def run_model_4(area_df: pd.DataFrame):
    """Model 4: score ~ public_school + hits + exam_code dummies + income dummies"""
    df = pd.get_dummies(
        area_df,
        columns=["exam_code", "income_bracket"],
        drop_first=True,
        dtype=int,
    )
    y = df["score"]
    X = df[
        ["public_school", "hits"]
        + [
            c
            for c in df.columns
            if c.startswith("exam_code_") or c.startswith("income_bracket_")
        ]
    ]
    return _run_ols(y, X)


def run_model_5(area_df: pd.DataFrame):
    """
    Model 5:
        score ~ public_school + hits + exam_code dummies
                + income dummies + race dummies + is_female
    """
    df = pd.get_dummies(
        area_df,
        columns=["exam_code", "income_bracket", "race_color"],
        drop_first=True,
        dtype=int,
    )
    y = df["score"]
    X = df[
        ["public_school", "hits", "is_female"]
        + [
            c
            for c in df.columns
            if c.startswith("exam_code_")
            or c.startswith("income_bracket_")
            or c.startswith("race_color_")
        ]
    ]
    return _run_ols(y, X)


# --------------------------------------------------------------------
# Summary table helpers
# --------------------------------------------------------------------


def _format_coef(model, var: str) -> str:
    """Return 'coef (se)***' style string for a given variable."""
    if var not in model.params:
        return ""
    coef = model.params[var]
    se = model.bse[var]
    p = model.pvalues[var]

    if p < 0.01:
        stars = "***"
    elif p < 0.05:
        stars = "**"
    elif p < 0.10:
        stars = "*"
    else:
        stars = ""

    return f"{coef:.3f} ({se:.3f}){stars}"


def summarize_models(
    models: Dict[str, sm.regression.linear_model.RegressionResultsWrapper],
    area: str,
    out_dir: Path = REG_TABLE_DIR,
) -> Path:
    """
    Build a regression table in English, similar to academic papers:

        (1) ... (5)
        Constant
        Public School
        Number of Correct Answers
        Exam Code Controls
        Income Controls
        Sex Control
        Race Controls
        R²
        N

    and save as CSV.
    """

    # Column labels as in research papers
    model_keys = ["model_1", "model_2", "model_3", "model_4", "model_5"]
    col_labels = ["(1)", "(2)", "(3)", "(4)", "(5)"]

    # Order of rows
    row_labels = [
        "Constant",
        "Public School",
        "Number of Correct Answers",
        "Exam Code Controls",
        "Income Controls",
        "Sex Control",
        "Race Controls",
        "R²",
        "N",
    ]

    # Which models include which controls
    control_flags = {
        "model_1": dict(exam=False, income=False, sex=False, race=False),
        "model_2": dict(exam=False, income=False, sex=False, race=False),
        "model_3": dict(exam=True, income=False, sex=False, race=False),
        "model_4": dict(exam=True, income=True, sex=False, race=False),
        "model_5": dict(exam=True, income=True, sex=True, race=True),
    }

    # Helper to format coefficient + standard error + stars
    def fmt(model, var: str) -> str:
        if var not in model.params.index:
            return ""
        coef = model.params[var]
        se = model.bse[var]
        p = model.pvalues[var]

        if p < 0.01:
            stars = "***"
        elif p < 0.05:
            stars = "**"
        elif p < 0.10:
            stars = "*"
        else:
            stars = ""

        return f"{coef:.2f}{stars} ({se:.2f})"

    # Build empty table
    table = pd.DataFrame(index=row_labels, columns=col_labels, dtype=object)

    # Fill table
    for key, col in zip(model_keys, col_labels):
        model = models[key]
        flags = control_flags[key]

        # Main coefficients
        table.loc["Constant", col] = fmt(model, "const")
        table.loc["Public School", col] = fmt(model, "public_school")
        table.loc["Number of Correct Answers", col] = fmt(model, "hits")

        # Controls
        table.loc["Exam Code Controls", col] = "Yes" if flags["exam"] else ""
        table.loc["Income Controls", col] = "Yes" if flags["income"] else ""
        table.loc["Sex Control", col] = "Yes" if flags["sex"] else ""
        table.loc["Race Controls", col] = "Yes" if flags["race"] else ""

        # Fit statistics
        table.loc["R²", col] = f"{model.rsquared:.2f}"
        table.loc["N", col] = f"{int(model.nobs):,}"

    # Save
    out_path = out_dir / f"regression_table_{area}.csv"
    table.to_csv(out_path, encoding="utf-8-sig")
    logging.info("Saved regression-style table for area %s to %s", area, out_path)

    return out_path


# --------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------


def run_all_regressions():
    """
    Run Models 1–5 for each exam area (CN, CH, LC, MT),
    save compact CSV tables, and return all fitted models.
    """
    base_df = load_base_df(HITS_PARQUET)

    all_models: Dict[
        str, Dict[str, sm.regression.linear_model.RegressionResultsWrapper]
    ] = {}

    for area in AREAS.keys():
        logging.info("Preparing data and running regressions for area %s", area)
        area_df = prepare_area_df(base_df, area)

        m1 = run_model_1(area_df)
        m2 = run_model_2(area_df)
        m3 = run_model_3(area_df)
        m4 = run_model_4(area_df)
        m5 = run_model_5(area_df)

        models_for_area = {
            "model_1": m1,
            "model_2": m2,
            "model_3": m3,
            "model_4": m4,
            "model_5": m5,
        }

        all_models[area] = models_for_area
        summarize_models(models_for_area, area, REG_TABLE_DIR)

    return all_models


if __name__ == "__main__":
    run_all_regressions()
