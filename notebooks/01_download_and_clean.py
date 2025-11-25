from __future__ import annotations

import logging
import time
import zipfile
from pathlib import Path

import polars as pl
import requests

# --------------------------------------------------------------------
# Paths & constants
# --------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "microdados_enem_2017"
PROC_DIR = PROJECT_ROOT / "data" / "processed" / "microdados_enem_2017"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

ZIP_URL = "https://download.inep.gov.br/microdados/microdados_enem_2017.zip"
ZIP_PATH = RAW_DIR / "microdados_enem_2017.zip"
CSV_PATH = RAW_DIR / "DADOS" / "MICRODADOS_ENEM_2017.csv"
OUT_PATH = PROC_DIR / "enem_2017.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --------------------------------------------------------------------
# Metadata
# --------------------------------------------------------------------

COLUMN_MAP = {
    "NU_INSCRICAO": ("registration_id", "Unique registration ID"),
    "NU_ANO": ("exam_year", "Exam year"),
    "TP_SEXO": ("sex", "Sex"),
    "TP_COR_RACA": ("race_color", "Race/color"),
    "TP_ST_CONCLUSAO": ("hs_completion_status", "HS completion status"),
    "TP_ESCOLA": ("school_type", "HS type"),
    "TP_ENSINO": ("teaching_mode", "HS modality"),
    "IN_TREINEIRO": ("is_tester", "Treineiro flag"),
    "TP_DEPENDENCIA_ADM_ESC": ("school_admin_dependency", "Admin dependency"),
    "TP_LOCALIZACAO_ESC": ("school_location", "Urban/Rural"),
    "TP_SIT_FUNC_ESC": ("school_oper_status", "Operational status"),
    "CO_PROVA_CN": ("code_exam_science", "Science booklet"),
    "CO_PROVA_CH": ("code_exam_humanities", "Humanities booklet"),
    "CO_PROVA_LC": ("code_exam_language", "Language booklet"),
    "CO_PROVA_MT": ("code_exam_math", "Math booklet"),
    "TP_PRESENCA_CN": ("presence_science", "Presence CN"),
    "TP_PRESENCA_CH": ("presence_humanities", "Presence CH"),
    "TP_PRESENCA_LC": ("presence_language", "Presence LC"),
    "TP_PRESENCA_MT": ("presence_math", "Presence MT"),
    "TX_RESPOSTAS_CN": ("answers_science", "Answers CN"),
    "TX_RESPOSTAS_CH": ("answers_humanities", "Answers CH"),
    "TX_RESPOSTAS_LC": ("answers_language", "Answers LC"),
    "TX_RESPOSTAS_MT": ("answers_math", "Answers MT"),
    "TX_GABARITO_CN": ("key_science", "Key CN"),
    "TX_GABARITO_CH": ("key_humanities", "Key CH"),
    "TX_GABARITO_LC": ("key_language", "Key LC"),
    "TX_GABARITO_MT": ("key_math", "Key MT"),
    "NU_NOTA_CN": ("score_science", "Score CN"),
    "NU_NOTA_CH": ("score_humanities", "Score CH"),
    "NU_NOTA_LC": ("score_language", "Score LC"),
    "NU_NOTA_MT": ("score_math", "Score MT"),
    "Q006": ("family_income_bracket", "Income bracket"),
    "Q027": ("school_funding_src", "Funding source"),
}

SCHEMA_OVERRIDES = {
    "NU_INSCRICAO": pl.Int64,
    "NU_ANO": pl.Int16,
    "TP_FAIXA_ETARIA": pl.Int8,
    "TP_SEXO": pl.Utf8,
    "TP_ESTADO_CIVIL": pl.Int8,
    "TP_COR_RACA": pl.Int8,
    "TP_NACIONALIDADE": pl.Int8,
    "TP_ST_CONCLUSAO": pl.Int8,
    "TP_ANO_CONCLUIU": pl.Int8,
    "TP_ESCOLA": pl.Int8,
    "TP_ENSINO": pl.Int8,
    "IN_TREINEIRO": pl.Int8,
    "CO_MUNICIPIO_ESC": pl.Int32,
    "NO_MUNICIPIO_ESC": pl.Utf8,
    "CO_UF_ESC": pl.Int8,
    "SG_UF_ESC": pl.Utf8,
    "TP_DEPENDENCIA_ADM_ESC": pl.Int8,
    "TP_LOCALIZACAO_ESC": pl.Int8,
    "TP_SIT_FUNC_ESC": pl.Int8,
    "CO_MUNICIPIO_PROVA": pl.Int32,
    "NO_MUNICIPIO_PROVA": pl.Utf8,
    "CO_UF_PROVA": pl.Int8,
    "SG_UF_PROVA": pl.Utf8,
    "CO_PROVA_CN": pl.Int16,
    "CO_PROVA_CH": pl.Int16,
    "CO_PROVA_LC": pl.Int16,
    "CO_PROVA_MT": pl.Int16,
    "TP_PRESENCA_CN": pl.Int8,
    "TP_PRESENCA_CH": pl.Int8,
    "TP_PRESENCA_LC": pl.Int8,
    "TP_PRESENCA_MT": pl.Int8,
    "TX_RESPOSTAS_CN": pl.Utf8,
    "TX_RESPOSTAS_CH": pl.Utf8,
    "TX_RESPOSTAS_LC": pl.Utf8,
    "TX_RESPOSTAS_MT": pl.Utf8,
    "TX_GABARITO_CN": pl.Utf8,
    "TX_GABARITO_CH": pl.Utf8,
    "TX_GABARITO_LC": pl.Utf8,
    "TX_GABARITO_MT": pl.Utf8,
    "NU_NOTA_CN": pl.Float64,
    "NU_NOTA_CH": pl.Float64,
    "NU_NOTA_LC": pl.Float64,
    "NU_NOTA_MT": pl.Float64,
    "Q006": pl.Utf8,
    "Q027": pl.Utf8,
}

SELECT_COLS = list(COLUMN_MAP.keys())
CATEGORICAL_COLS = ["sex", "family_income_bracket", "school_funding_src"]


# --------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------


def download() -> Path:
    """Download the ZIP if not already present."""
    if ZIP_PATH.exists():
        logging.info("ZIP already exists: %s", ZIP_PATH)
        return ZIP_PATH

    logging.info("Downloading ENEM 2017 microdata...")
    resp = requests.get(ZIP_URL, stream=True, timeout=30)
    resp.raise_for_status()

    with ZIP_PATH.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    logging.info("Download completed.")
    return ZIP_PATH


def extract(zip_path: Path = ZIP_PATH) -> Path:
    """Extract the ZIP to RAW_DIR if not already extracted."""
    target_dir = RAW_DIR / "microdados_enem_2017"
    if target_dir.exists():
        logging.info("Data already extracted: %s", target_dir)
        return target_dir

    logging.info("Extracting %s ...", zip_path)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(RAW_DIR)

    logging.info("Extraction completed.")
    return target_dir


def _transform(df: pl.DataFrame) -> pl.DataFrame:
    """Rename columns and encode key categorical variables."""
    df = df.rename({old: new for old, (new, _) in COLUMN_MAP.items()})

    for col in CATEGORICAL_COLS:
        if col in df.columns:
            df = df.with_columns(pl.col(col).rank("dense").cast(pl.Int16).alias(col))

    return df


def build() -> Path:
    """Load CSV, transform, save as Parquet, and print size reduction."""
    t0 = time.perf_counter()
    logging.info("Reading CSV %s ...", CSV_PATH)

    df = pl.read_csv(
        CSV_PATH,
        separator=";",
        encoding="iso-8859-1",
        columns=SELECT_COLS,
        schema_overrides=SCHEMA_OVERRIDES,
    )

    original_size = CSV_PATH.stat().st_size

    df = _transform(df)

    df = df.filter(
        (pl.col("presence_science") == 1)
        & (pl.col("presence_humanities") == 1)
        & (pl.col("presence_language") == 1)
        & (pl.col("presence_math") == 1)
    )

    df.write_parquet(OUT_PATH)

    parquet_size = OUT_PATH.stat().st_size
    reduction = 100 * (1 - parquet_size / original_size)

    logging.info(
        "Saved Parquet to %s — %d rows (%.1fs)",
        OUT_PATH.relative_to(PROJECT_ROOT),
        df.height,
        time.perf_counter() - t0,
    )

    logging.info(
        "File size reduced from %.2f MB to %.2f MB (%.1f%% reduction)",
        original_size / 1e6,
        parquet_size / 1e6,
        reduction,
    )

    return OUT_PATH


if __name__ == "__main__":
    zip_path = download()
    extract(zip_path)
    build()
