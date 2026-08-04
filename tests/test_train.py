import json
from pathlib import Path

import joblib
import pandas as pd

from car_price_ml.train import train_and_evaluate


def synthetic_dataset(rows: int = 150) -> pd.DataFrame:
    models = ["Honda Civic", "Toyota Corolla", "Suzuki Alto", "KIA Sportage"]
    cities = ["Lahore", "Karachi", "Islamabad", "Rawalpindi"]
    records = []
    for index in range(rows):
        year = 2000 + index % 25
        engine = 660 + (index % 8) * 200
        mileage = 2_000 + index * 1_250
        price = 500_000 + year * 1_000 + engine * 600 - mileage * 0.8
        records.append(
            {
                "title": f"{models[index % len(models)]} {year} for sale in "
                f"{cities[index % len(cities)]}",
                "price": f"PKR {price:,.0f}",
                "year": year,
                "mileage": f"{mileage:,} km",
                "fuel_type": "Petrol" if index % 3 else "Hybrid",
                "engine_capacity": f"{engine} cc",
                "transmission": "Automatic" if index % 2 else "Manual",
                "link": f"https://example.test/cars/{index}",
            }
        )
    return pd.DataFrame(records)


def test_training_command_writes_valid_artifact_and_report(tmp_path: Path):
    data_path = tmp_path / "cars.csv"
    model_path = tmp_path / "model.joblib"
    metrics_path = tmp_path / "metrics.json"
    synthetic_dataset().to_csv(data_path, index=False)

    report = train_and_evaluate(
        data_path=data_path,
        model_path=model_path,
        metrics_path=metrics_path,
        n_estimators=2,
        random_state=7,
    )

    assert report["dataset"]["cleaning"]["clean_rows"] == 150
    assert report["split"]["feature_group_overlap"] == 0
    assert report["split"]["train_rows"] + report["split"]["test_rows"] == 150
    assert model_path.is_file()
    assert metrics_path.is_file()
    assert json.loads(metrics_path.read_text())["schema_version"] == 1
    artifact = joblib.load(model_path)
    assert artifact["artifact_version"] == 1
    assert hasattr(artifact["pipeline"], "predict")
