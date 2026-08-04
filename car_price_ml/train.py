"""Command-line training entry point with leakage-aware holdout evaluation."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

import joblib
import numpy as np
import xgboost
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.dummy import DummyRegressor
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor

from .constants import (
    CATEGORICAL_FEATURES,
    DEFAULT_DATA_PATH,
    DEFAULT_METRICS_PATH,
    DEFAULT_MODEL_PATH,
    FEATURE_COLUMNS,
    NUMERIC_FEATURES,
    PROJECT_ROOT,
    TARGET_COLUMN,
)
from .data import feature_groups, file_sha256, load_raw_dataset, prepare_dataset
from .evaluation import metrics_by_price_band, regression_metrics


def build_pipeline(*, n_estimators: int, random_state: int) -> Pipeline:
    preprocess = ColumnTransformer(
        transformers=[
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", min_frequency=2),
                CATEGORICAL_FEATURES,
            ),
            ("numeric", "passthrough", NUMERIC_FEATURES),
        ]
    )
    regressor = XGBRegressor(
        objective="reg:squarederror",
        n_estimators=n_estimators,
        learning_rate=0.05,
        max_depth=6,
        min_child_weight=2,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        random_state=random_state,
        tree_method="hist",
        device="cpu",
        n_jobs=-1,
    )
    transformed_target = TransformedTargetRegressor(
        regressor=regressor,
        func=np.log1p,
        inverse_func=np.expm1,
        check_inverse=False,
    )
    return Pipeline([("preprocess", preprocess), ("model", transformed_target)])


def _atomic_joblib_dump(value: object, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    try:
        joblib.dump(value, temporary_path)
        temporary_path.replace(path)
        path.chmod(0o644)
    finally:
        temporary_path.unlink(missing_ok=True)


def _atomic_json_dump(value: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".json", dir=path.parent, text=True
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary_path.replace(path)
        path.chmod(0o644)
    finally:
        temporary_path.unlink(missing_ok=True)


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def train_and_evaluate(
    *,
    data_path: Path,
    model_path: Path,
    metrics_path: Path,
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 500,
) -> dict:
    raw = load_raw_dataset(data_path)
    clean, cleaning_report = prepare_dataset(raw)
    if len(clean) < 100:
        raise ValueError("At least 100 clean rows are required to train and evaluate the model.")

    groups = feature_groups(clean)
    splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=random_state)
    train_indices, test_indices = next(splitter.split(clean, groups=groups))
    train_frame = clean.iloc[train_indices]
    test_frame = clean.iloc[test_indices]

    train_groups = set(groups.iloc[train_indices])
    test_groups = set(groups.iloc[test_indices])
    group_overlap = len(train_groups.intersection(test_groups))
    if group_overlap:
        raise RuntimeError(f"Grouped split leaked {group_overlap} feature signatures.")

    x_train = train_frame[FEATURE_COLUMNS]
    y_train = train_frame[TARGET_COLUMN]
    x_test = test_frame[FEATURE_COLUMNS]
    y_test = test_frame[TARGET_COLUMN].to_numpy()

    model = build_pipeline(
        n_estimators=n_estimators,
        random_state=random_state,
    )
    model.fit(x_train, y_train)
    predictions = model.predict(x_test)

    baseline = DummyRegressor(strategy="median")
    baseline.fit(np.zeros((len(y_train), 1)), y_train)
    baseline_predictions = baseline.predict(np.zeros((len(y_test), 1)))

    trained_at = datetime.now(UTC).isoformat()
    model_metrics = regression_metrics(y_test, predictions)
    baseline_metrics = regression_metrics(y_test, baseline_predictions)
    one_hot = model.named_steps["preprocess"].named_transformers_["categorical"]
    known_categories = {
        name: [str(value) for value in values]
        for name, values in zip(CATEGORICAL_FEATURES, one_hot.categories_, strict=True)
    }
    package_versions = {
        package: version(package)
        for package in ("django", "joblib", "numpy", "pandas", "scikit-learn")
    }
    package_versions["xgboost"] = xgboost.__version__

    report = {
        "schema_version": 1,
        "generated_at_utc": trained_at,
        "dataset": {
            "path": _display_path(data_path),
            "sha256": file_sha256(data_path),
            "cleaning": cleaning_report.to_dict(),
        },
        "split": {
            "strategy": "GroupShuffleSplit grouped by identical model features",
            "random_state": random_state,
            "requested_test_fraction": test_size,
            "train_rows": int(len(train_frame)),
            "test_rows": int(len(test_frame)),
            "feature_group_overlap": group_overlap,
        },
        "model": {
            "type": "OneHotEncoder + log-target XGBRegressor",
            "parameters": {
                "n_estimators": n_estimators,
                "learning_rate": 0.05,
                "max_depth": 6,
                "device": "cpu",
            },
            "metrics": model_metrics,
            "metrics_by_price_band": metrics_by_price_band(y_test, predictions),
        },
        "median_baseline": {"metrics": baseline_metrics},
        "environment": package_versions,
    }
    artifact = {
        "artifact_version": 1,
        "pipeline": model,
        "metadata": {
            "trained_at_utc": trained_at,
            "feature_columns": FEATURE_COLUMNS,
            "known_categories": known_categories,
            "dataset_sha256": report["dataset"]["sha256"],
            "metrics": model_metrics,
            "environment": package_versions,
        },
    }
    _atomic_joblib_dump(artifact, model_path)
    report["model_artifact"] = {
        "path": _display_path(model_path),
        "sha256": file_sha256(model_path),
    }
    _atomic_json_dump(report, metrics_path)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_MODEL_PATH)
    parser.add_argument("--metrics", type=Path, default=DEFAULT_METRICS_PATH)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--n-estimators", type=int, default=500)
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    if not 0.05 <= arguments.test_size <= 0.5:
        raise SystemExit("--test-size must be between 0.05 and 0.5")
    report = train_and_evaluate(
        data_path=arguments.data,
        model_path=arguments.output,
        metrics_path=arguments.metrics,
        test_size=arguments.test_size,
        random_state=arguments.random_state,
        n_estimators=arguments.n_estimators,
    )
    summary = {
        "clean_rows": report["dataset"]["cleaning"]["clean_rows"],
        "train_rows": report["split"]["train_rows"],
        "test_rows": report["split"]["test_rows"],
        "metrics": report["model"]["metrics"],
        "model_path": report["model_artifact"]["path"],
        "metrics_path": _display_path(arguments.metrics),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
