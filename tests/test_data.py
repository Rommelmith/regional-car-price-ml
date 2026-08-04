import pandas as pd

from car_price_ml.data import feature_groups, prepare_dataset


def make_row(**overrides):
    row = {
        "title": "Honda Civic 2020 for sale in Lahore",
        "price": "PKR 4,500,000",
        "year": 2020,
        "mileage": "65,000 km",
        "fuel_type": "Petrol",
        "engine_capacity": "1800 cc",
        "transmission": "Automatic",
        "link": "https://example.test/cars/1",
    }
    row.update(overrides)
    return row


def test_prepare_dataset_cleans_filters_and_deduplicates():
    raw = pd.DataFrame(
        [
            make_row(),
            make_row(price="PKR 4,700,000"),
            make_row(link="https://example.test/cars/electric", fuel_type="Electric"),
            make_row(link="https://example.test/cars/missing", mileage=None),
            make_row(link="https://example.test/cars/old", year=1940),
        ]
    )

    clean, report = prepare_dataset(raw, reference_year=2026)

    assert len(clean) == 1
    assert clean.iloc[0]["title"] == "Honda Civic"
    assert clean.iloc[0]["city"] == "Lahore"
    assert clean.iloc[0]["price"] == 4_500_000
    assert report.unsupported_powertrain_rows_removed == 1
    assert report.duplicate_links_removed == 1
    assert report.invalid_or_missing_feature_rows_removed == 2


def test_identical_model_inputs_share_a_group_even_with_different_links():
    raw = pd.DataFrame(
        [make_row(), make_row(link="https://example.test/cars/2", price="PKR 4,700,000")]
    )
    clean, _ = prepare_dataset(raw, reference_year=2026)

    groups = feature_groups(clean)

    assert groups.nunique() == 1
