"""Lazy, validated access to the serialized prediction pipeline."""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from django.conf import settings

from car_price_ml.constants import FEATURE_COLUMNS


class ModelUnavailableError(RuntimeError):
    """Raised when no usable model artifact is available."""


class ModelPredictionError(RuntimeError):
    """Raised when the pipeline returns an invalid value."""


@dataclass(frozen=True)
class PredictionResult:
    price: float
    warnings: list[str]


_model_lock = threading.Lock()
_cached_bundle: dict[str, Any] | None = None
_cached_signature: tuple[Path, int] | None = None


def reset_model_cache() -> None:
    """Clear process-local model state; primarily useful for tests and artifact reloads."""

    global _cached_bundle, _cached_signature
    with _model_lock:
        _cached_bundle = None
        _cached_signature = None


def _normalize_artifact(value: Any) -> dict[str, Any]:
    if isinstance(value, dict) and hasattr(value.get("pipeline"), "predict"):
        return value
    if hasattr(value, "predict"):
        return {"artifact_version": 0, "pipeline": value, "metadata": {}}
    raise ModelUnavailableError("The model artifact does not contain a prediction pipeline.")


def load_model_bundle() -> dict[str, Any]:
    global _cached_bundle, _cached_signature
    path = Path(settings.CAR_PRICE_MODEL_PATH)
    try:
        signature = (path, path.stat().st_mtime_ns)
    except FileNotFoundError as exc:
        raise ModelUnavailableError(
            "Model artifact is missing. Run `python -m car_price_ml.train`."
        ) from exc

    if _cached_bundle is not None and _cached_signature == signature:
        return _cached_bundle
    with _model_lock:
        if _cached_bundle is not None and _cached_signature == signature:
            return _cached_bundle
        try:
            bundle = _normalize_artifact(joblib.load(path))
        except ModelUnavailableError:
            raise
        except Exception as exc:
            raise ModelUnavailableError("The model artifact could not be loaded.") from exc
        _cached_bundle = bundle
        _cached_signature = signature
        return bundle


def get_model_metadata() -> dict[str, Any]:
    return dict(load_model_bundle().get("metadata") or {})


def _canonical_category(value: str, known: list[str]) -> tuple[str, bool]:
    lookup = {candidate.casefold(): candidate for candidate in known}
    canonical = lookup.get(value.casefold())
    return (canonical, True) if canonical is not None else (value, False)


def predict_price(cleaned_data: dict[str, Any]) -> PredictionResult:
    bundle = load_model_bundle()
    metadata = bundle.get("metadata") or {}
    known_categories = metadata.get("known_categories") or {}
    values = dict(cleaned_data)
    warnings: list[str] = []

    for field, label in (("title", "car model"), ("city", "city")):
        known = [str(item) for item in known_categories.get(field, [])]
        if not known:
            continue
        values[field], recognized = _canonical_category(str(values[field]), known)
        if not recognized:
            warnings.append(
                f"The {label} was not represented in training data; this estimate is less reliable."
            )

    frame = pd.DataFrame([{column: values[column] for column in FEATURE_COLUMNS}])
    try:
        prediction = float(bundle["pipeline"].predict(frame)[0])
    except Exception as exc:
        raise ModelPredictionError("The model could not generate a prediction.") from exc
    if not math.isfinite(prediction) or prediction < 0:
        raise ModelPredictionError("The model returned an invalid price.")
    return PredictionResult(price=prediction, warnings=warnings)
