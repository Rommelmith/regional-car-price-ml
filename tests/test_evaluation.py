import numpy as np

from car_price_ml.evaluation import metrics_by_price_band, regression_metrics


def test_regression_metrics_are_calculated_from_predictions():
    metrics = regression_metrics(np.array([100.0, 200.0]), np.array([110.0, 180.0]))

    assert metrics["mae_pkr"] == 15.0
    assert metrics["median_ae_pkr"] == 15.0
    assert metrics["within_10_percent"] == 100.0


def test_price_band_metrics_include_observation_counts():
    output = metrics_by_price_band(
        np.array([800_000.0, 2_000_000.0, 20_000_000.0]),
        np.array([750_000.0, 2_100_000.0, 18_000_000.0]),
    )

    assert output["under_1m"]["count"] == 1
    assert output["1m_to_3m"]["count"] == 1
    assert output["15m_and_above"]["count"] == 1
