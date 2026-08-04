"""Shared model schema and validation limits."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "pakwheels_cars.csv"
DEFAULT_MODEL_PATH = PROJECT_ROOT / "artifacts" / "car_price_pipeline.joblib"
DEFAULT_METRICS_PATH = PROJECT_ROOT / "reports" / "model_metrics.json"

TARGET_COLUMN = "price"
CATEGORICAL_FEATURES = ["title", "fuel_type", "city", "transmission"]
NUMERIC_FEATURES = ["mileage", "engine_capacity", "year"]
FEATURE_COLUMNS = [
    "title",
    "year",
    "mileage",
    "engine_capacity",
    "city",
    "transmission",
    "fuel_type",
]

RAW_COLUMNS = {
    "title",
    "price",
    "year",
    "mileage",
    "fuel_type",
    "engine_capacity",
    "transmission",
    "link",
}

SUPPORTED_FUEL_TYPES = ("Petrol", "Diesel", "CNG", "Hybrid", "LPG", "PHEV")
SUPPORTED_TRANSMISSIONS = ("Automatic", "Manual")

MIN_YEAR = 1950
MAX_YEAR_OFFSET = 1
MIN_MILEAGE = 0
MAX_MILEAGE = 2_000_000
MIN_ENGINE_CAPACITY = 300
MAX_ENGINE_CAPACITY = 10_000
