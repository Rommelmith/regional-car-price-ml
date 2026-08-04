"""Dataset loading, validation, and deterministic feature preparation."""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from .constants import (
    FEATURE_COLUMNS,
    MAX_ENGINE_CAPACITY,
    MAX_MILEAGE,
    MAX_YEAR_OFFSET,
    MIN_ENGINE_CAPACITY,
    MIN_MILEAGE,
    MIN_YEAR,
    RAW_COLUMNS,
    SUPPORTED_FUEL_TYPES,
    SUPPORTED_TRANSMISSIONS,
    TARGET_COLUMN,
)


class DataValidationError(ValueError):
    """Raised when an input dataset does not match the expected schema."""


@dataclass(frozen=True)
class CleaningReport:
    """Auditable row counts from each cleaning stage."""

    raw_rows: int
    missing_core_rows_removed: int
    unsupported_powertrain_rows_removed: int
    duplicate_links_removed: int
    invalid_or_missing_feature_rows_removed: int
    clean_rows: int
    generated_at_utc: str

    def to_dict(self) -> dict[str, int | str]:
        return asdict(self)


def load_raw_dataset(path: str | Path) -> pd.DataFrame:
    """Load the scraper CSV and verify its required columns."""

    dataset_path = Path(path)
    if not dataset_path.is_file():
        raise DataValidationError(
            f"Dataset not found at {dataset_path}. Run the scraper or provide --data."
        )
    frame = pd.read_csv(dataset_path)
    validate_raw_columns(frame)
    return frame


def validate_raw_columns(frame: pd.DataFrame) -> None:
    missing = sorted(RAW_COLUMNS.difference(frame.columns))
    if missing:
        raise DataValidationError(f"Dataset is missing required columns: {', '.join(missing)}")


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.replace(r"\s+", " ", regex=True).str.strip()


def _parse_number(series: pd.Series, unit_pattern: str = "") -> pd.Series:
    values = series.astype("string").str.replace(",", "", regex=False)
    if unit_pattern:
        values = values.str.replace(unit_pattern, "", regex=True, case=False)
    extracted = values.str.extract(r"([-+]?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(extracted, errors="coerce")


def prepare_dataset(
    raw: pd.DataFrame,
    *,
    reference_year: int | None = None,
) -> tuple[pd.DataFrame, CleaningReport]:
    """Convert scraper output into the exact schema consumed by the model.

    Rows are deduplicated by listing URL before splitting so one advertisement cannot appear in
    both the training and holdout partitions.
    """

    validate_raw_columns(raw)
    year_now = reference_year or datetime.now(UTC).year
    frame = raw.copy()
    raw_rows = len(frame)

    core_columns = ["title", "price", "year", "link"]
    core_present = frame[core_columns].notna().all(axis=1)
    for column in ("title", "price", "link"):
        core_present &= _normalize_text(frame[column]).ne("")
    frame = frame.loc[core_present].copy()
    missing_core_removed = raw_rows - len(frame)

    fuel = _normalize_text(frame["fuel_type"])
    electric = fuel.str.casefold().eq("electric").fillna(False)
    battery_capacity = (
        frame["engine_capacity"].astype("string").str.contains("kWh", case=False, na=False)
    )
    before_powertrain = len(frame)
    supported_powertrain = fuel.notna() & ~(electric | battery_capacity)
    frame = frame.loc[supported_powertrain].copy()
    unsupported_powertrain_removed = before_powertrain - len(frame)

    before_deduplication = len(frame)
    frame = frame.drop_duplicates(subset=["link"], keep="first").copy()
    duplicate_links_removed = before_deduplication - len(frame)

    original_title = _normalize_text(frame["title"])
    frame["city"] = original_title.str.extract(r"(?i)\s+for sale in\s+(.+?)\s*$", expand=False)
    frame["title"] = (
        original_title.str.replace(r"(?i)\s+for sale in\s+.+$", "", regex=True)
        .str.replace(r"\b(?:19|20)\d{2}\b", "", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    frame["city"] = _normalize_text(frame["city"])
    frame["fuel_type"] = _normalize_text(frame["fuel_type"])
    frame["transmission"] = _normalize_text(frame["transmission"])
    frame["price"] = _parse_number(frame["price"], r"\bPKR\b")
    frame["mileage"] = _parse_number(frame["mileage"], r"\bkm\b")
    frame["engine_capacity"] = _parse_number(frame["engine_capacity"], r"\bcc\b")
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce")

    required = FEATURE_COLUMNS + [TARGET_COLUMN, "link"]
    valid = frame[required].notna().all(axis=1)
    valid &= frame["title"].ne("") & frame["city"].ne("")
    valid &= frame["price"].gt(0)
    valid &= frame["year"].between(MIN_YEAR, year_now + MAX_YEAR_OFFSET)
    valid &= frame["mileage"].between(MIN_MILEAGE, MAX_MILEAGE)
    valid &= frame["engine_capacity"].between(MIN_ENGINE_CAPACITY, MAX_ENGINE_CAPACITY)
    valid &= frame["fuel_type"].isin(SUPPORTED_FUEL_TYPES)
    valid &= frame["transmission"].isin(SUPPORTED_TRANSMISSIONS)

    before_validation = len(frame)
    frame = frame.loc[valid, required].copy()
    invalid_removed = before_validation - len(frame)

    frame["year"] = frame["year"].astype("int64")
    frame = frame.reset_index(drop=True)

    report = CleaningReport(
        raw_rows=raw_rows,
        missing_core_rows_removed=missing_core_removed,
        unsupported_powertrain_rows_removed=unsupported_powertrain_removed,
        duplicate_links_removed=duplicate_links_removed,
        invalid_or_missing_feature_rows_removed=invalid_removed,
        clean_rows=len(frame),
        generated_at_utc=datetime.now(UTC).isoformat(),
    )
    return frame, report


def feature_groups(frame: pd.DataFrame) -> pd.Series:
    """Return stable hashes used to keep identical feature rows in one split."""

    missing = set(FEATURE_COLUMNS).difference(frame.columns)
    if missing:
        raise DataValidationError(f"Cannot group features; missing: {', '.join(sorted(missing))}")
    return pd.util.hash_pandas_object(frame[FEATURE_COLUMNS], index=False).astype("string")


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def slugify_identifier(value: str) -> str:
    """Normalize identifiers used only in reports and tests."""

    return re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
