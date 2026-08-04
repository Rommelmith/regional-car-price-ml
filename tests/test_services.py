from pathlib import Path

import joblib
import numpy as np
from django.test import override_settings

from predictor.services import ModelUnavailableError, predict_price, reset_model_cache


class ConstantPipeline:
    def predict(self, frame):
        assert list(frame.columns) == [
            "title",
            "year",
            "mileage",
            "engine_capacity",
            "city",
            "transmission",
            "fuel_type",
        ]
        return np.array([4_200_000.0])


def payload(**overrides):
    values = {
        "title": "honda civic",
        "year": 2020,
        "mileage": 65_000,
        "engine_capacity": 1800,
        "city": "lahore",
        "transmission": "Automatic",
        "fuel_type": "Petrol",
    }
    values.update(overrides)
    return values


def write_artifact(path: Path):
    joblib.dump(
        {
            "artifact_version": 1,
            "pipeline": ConstantPipeline(),
            "metadata": {
                "known_categories": {
                    "title": ["Honda Civic"],
                    "city": ["Lahore"],
                }
            },
        },
        path,
    )


def test_prediction_canonicalizes_known_categories(tmp_path: Path):
    artifact = tmp_path / "model.joblib"
    write_artifact(artifact)
    reset_model_cache()

    with override_settings(CAR_PRICE_MODEL_PATH=artifact):
        result = predict_price(payload())

    assert result.price == 4_200_000
    assert result.warnings == []


def test_prediction_warns_for_unknown_model(tmp_path: Path):
    artifact = tmp_path / "model.joblib"
    write_artifact(artifact)
    reset_model_cache()

    with override_settings(CAR_PRICE_MODEL_PATH=artifact):
        result = predict_price(payload(title="Unknown Model"))

    assert len(result.warnings) == 1


def test_missing_artifact_has_actionable_error(tmp_path: Path):
    reset_model_cache()
    with override_settings(CAR_PRICE_MODEL_PATH=tmp_path / "missing.joblib"):
        try:
            predict_price(payload())
        except ModelUnavailableError as error:
            assert "car_price_ml.train" in str(error)
        else:
            raise AssertionError("ModelUnavailableError was not raised")
