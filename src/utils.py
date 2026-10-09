"""Small shared helpers: custom errors, saving tables/JSON, logging."""
import json
import logging
from pathlib import Path

import pandas as pd

from src import config


class DatasetNotFoundError(Exception):
    pass


class SchemaError(Exception):
    pass


class EmptyDatasetError(Exception):
    pass


def get_logger(name):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s",
                        datefmt="%H:%M:%S")
    return logging.getLogger(name)


def save_table(pdf: pd.DataFrame, name: str) -> Path:
    """Save a SMALL pandas table (aggregated Spark output) as CSV."""
    config.TABLES_DIR.mkdir(parents=True, exist_ok=True)
    path = config.TABLES_DIR / f"{name}.csv"
    pdf.to_csv(path, index=False)
    return path


def save_parquet_table(pdf: pd.DataFrame, name: str) -> Path:
    config.TABLES_DIR.mkdir(parents=True, exist_ok=True)
    path = config.TABLES_DIR / f"{name}.parquet"
    pdf.to_parquet(path, index=False)
    return path


def save_json(obj, name: str) -> Path:
    config.TABLES_DIR.mkdir(parents=True, exist_ok=True)
    path = config.TABLES_DIR / f"{name}.json"
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    return path


def update_json(updates: dict, name: str) -> Path:
    path = config.TABLES_DIR / f"{name}.json"
    current = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    current.update(updates)
    return save_json(current, name)
