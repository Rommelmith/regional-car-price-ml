from pathlib import Path

from car_price_ml.scrape import (
    load_existing_rows,
    parse_engine_specs,
    parse_search_page,
    write_csv_atomic,
)

HTML = """
<ul class="search-results">
  <li class="classified-listing">
    <script type="application/ld+json">
      {"name": "Honda Civic 2020 for sale in Lahore", "modelDate": 2020,
       "offers": {"price": 4500000, "priceCurrency": "PKR", "url": "https://example.test/1"}}
    </script>
    <ul class="ad-specs">
      <li><i class="pw-mileage"></i>65,000 km</li>
      <li><i class="pw-engine"></i>Petrol . 1800 cc . Automatic</li>
    </ul>
  </li>
</ul>
"""


def test_parse_engine_specs():
    assert parse_engine_specs("Petrol . 1800 cc . Automatic") == (
        "Petrol",
        "1800 cc",
        "Automatic",
    )


def test_parse_search_page_extracts_listing():
    rows = parse_search_page(HTML)

    assert rows == [
        {
            "title": "Honda Civic 2020 for sale in Lahore",
            "price": "PKR 4,500,000",
            "year": "2020",
            "mileage": "65,000 km",
            "fuel_type": "Petrol",
            "engine_capacity": "1800 cc",
            "transmission": "Automatic",
            "link": "https://example.test/1",
        }
    ]


def test_atomic_csv_round_trip(tmp_path: Path):
    output = tmp_path / "nested" / "cars.csv"
    rows = parse_search_page(HTML)

    write_csv_atomic(rows, output)

    assert load_existing_rows(output) == rows
    assert not list(output.parent.glob("*.partial.csv"))
