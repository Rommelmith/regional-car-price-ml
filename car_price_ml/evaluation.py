"""Regression metrics used by training and documented model reports."""

from __future__ import annotations

import math

import numpy as np
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    median_absolute_error,
    r2_score,
)


def regression_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float | None]:
    actual_values = np.asarray(actual, dtype=float)
    predicted_values = np.asarray(predicted, dtype=float)
    absolute_percentage_error = np.abs((actual_values - predicted_values) / actual_values)
    relative_error = absolute_percentage_error
    return {
        "mae_pkr": float(mean_absolute_error(actual_values, predicted_values)),
        "median_ae_pkr": float(median_absolute_error(actual_values, predicted_values)),
        "rmse_pkr": float(math.sqrt(mean_squared_error(actual_values, predicted_values))),
        "r2": float(r2_score(actual_values, predicted_values)) if len(actual_values) > 1 else None,
        "mape_percent": float(np.mean(absolute_percentage_error) * 100),
        "within_10_percent": float(np.mean(relative_error <= 0.10) * 100),
        "within_20_percent": float(np.mean(relative_error <= 0.20) * 100),
    }


def metrics_by_price_band(actual: np.ndarray, predicted: np.ndarray) -> dict[str, dict]:
    actual_values = np.asarray(actual, dtype=float)
    predicted_values = np.asarray(predicted, dtype=float)
    bands = (
        ("under_1m", 0, 1_000_000),
        ("1m_to_3m", 1_000_000, 3_000_000),
        ("3m_to_7m", 3_000_000, 7_000_000),
        ("7m_to_15m", 7_000_000, 15_000_000),
        ("15m_and_above", 15_000_000, np.inf),
    )
    output: dict[str, dict] = {}
    for name, lower, upper in bands:
        mask = (actual_values >= lower) & (actual_values < upper)
        if not np.any(mask):
            continue
        output[name] = {
            "count": int(mask.sum()),
            **regression_metrics(actual_values[mask], predicted_values[mask]),
        }
    return output
