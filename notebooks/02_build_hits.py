from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb

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

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROC_DIR = PROJECT_ROOT / "data" / "processed" / "microdados_enem_2017"
PROC_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_PARQUET = PROC_DIR / "enem_2017.parquet"
HITS_PARQUET = PROC_DIR / "enem_2017_hits.parquet"


# --------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------


def compute_hits(
    parquet_path: Path = SOURCE_PARQUET,
    out_path: Path = HITS_PARQUET,
    sample_size: int | None = None,
) -> Path:
    """
    Read the processed ENEM parquet file, compute number of correct answers
    per subject and median scores by (year, hits), then save to a new parquet.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Source parquet not found: {parquet_path}")

    parquet_path_sql = str(parquet_path).replace("\\", "\\\\")
    limit_clause = f"LIMIT {sample_size}" if sample_size is not None else ""

    logging.info("Computing hits from %s", parquet_path)
    t0 = time.perf_counter()

    query = f"""
WITH answers AS (
    SELECT
        registration_id,
        exam_year,
        UNNEST(STRING_SPLIT(COALESCE(answers_science, REPEAT('#', 45)), '')) AS ans_science,
        UNNEST(STRING_SPLIT(COALESCE(key_science, REPEAT('.', 45)), '')) AS key_science,
        UNNEST(STRING_SPLIT(COALESCE(answers_humanities, REPEAT('#', 45)), '')) AS ans_humanities,
        UNNEST(STRING_SPLIT(COALESCE(key_humanities, REPEAT('.', 45)), '')) AS key_humanities,
        UNNEST(STRING_SPLIT(COALESCE(answers_language, REPEAT('#', 50)), '')) AS ans_language,
        UNNEST(STRING_SPLIT(COALESCE(key_language, REPEAT('.', 50)), '')) AS key_language,
        UNNEST(STRING_SPLIT(COALESCE(answers_math, REPEAT('#', 45)), '')) AS ans_math,
        UNNEST(STRING_SPLIT(COALESCE(key_math, REPEAT('.', 45)), '')) AS key_math,
        score_science,
        score_humanities,
        score_language,
        score_math,
        code_exam_science,
        code_exam_humanities,
        code_exam_language,
        code_exam_math
    FROM read_parquet('{parquet_path_sql}')
    {limit_clause}
),

hits AS (
    SELECT
        registration_id,
        exam_year,
        score_science,
        score_humanities,
        score_language,
        score_math,
        code_exam_science,
        code_exam_humanities,
        code_exam_language,
        code_exam_math,
        SUM(CASE WHEN ans_science = key_science    THEN 1 ELSE 0 END) AS hits_science,
        SUM(CASE WHEN ans_humanities = key_humanities THEN 1 ELSE 0 END) AS hits_humanities,
        SUM(CASE WHEN ans_language   = key_language   THEN 1 ELSE 0 END) AS hits_language,
        SUM(CASE WHEN ans_math       = key_math       THEN 1 ELSE 0 END) AS hits_math
    FROM answers
    GROUP BY ALL
)

SELECT
    h.*,
    MEDIAN(h.score_science)  OVER (PARTITION BY h.exam_year, h.hits_science)    AS median_score_science,
    MEDIAN(h.score_humanities) OVER (PARTITION BY h.exam_year, h.hits_humanities) AS median_score_humanities,
    MEDIAN(h.score_language) OVER (PARTITION BY h.exam_year, h.hits_language)   AS median_score_language,
    MEDIAN(h.score_math)     OVER (PARTITION BY h.exam_year, h.hits_math)       AS median_score_math,

    CASE
        WHEN h.score_science >= MEDIAN(h.score_science)
             OVER (PARTITION BY h.exam_year, h.hits_science)
        THEN 1 ELSE 0
    END AS above_median_science,

    CASE
        WHEN h.score_humanities >= MEDIAN(h.score_humanities)
             OVER (PARTITION BY h.exam_year, h.hits_humanities)
        THEN 1 ELSE 0
    END AS above_median_humanities,

    CASE
        WHEN h.score_language >= MEDIAN(h.score_language)
             OVER (PARTITION BY h.exam_year, h.hits_language)
        THEN 1 ELSE 0
    END AS above_median_language,

    CASE
        WHEN h.score_math >= MEDIAN(h.score_math)
             OVER (PARTITION BY h.exam_year, h.hits_math)
        THEN 1 ELSE 0
    END AS above_median_math,

    t.sex,
    t.race_color,
    t.school_type,
    t.teaching_mode,
    t.presence_science,
    t.presence_humanities,
    t.presence_language,
    t.presence_math,
    t.family_income_bracket,
    t.school_funding_src,
    t.school_admin_dependency

FROM hits h
LEFT JOIN (
    SELECT *
    FROM read_parquet('{parquet_path_sql}')
    {limit_clause}
) AS t
ON h.registration_id = t.registration_id
   AND h.exam_year = t.exam_year
"""  # noqa: E501

    con = duckdb.connect(database=":memory:")
    df = con.execute(query).pl()

    df.write_parquet(out_path)

    elapsed = time.perf_counter() - t0
    logging.info(
        "Hits parquet saved to %s — %d rows (%.1fs)",
        out_path.relative_to(PROJECT_ROOT),
        df.height,
        elapsed,
    )

    return out_path


if __name__ == "__main__":
    compute_hits()
