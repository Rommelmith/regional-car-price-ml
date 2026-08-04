# Data card

## Source and intended use

The dataset contains public used-car advertisements collected from PakWheels search-result pages. It is intended for educational analysis of **asking prices in Pakistan** and for demonstrating a reproducible ML workflow. It is not a record of completed sales and must not be represented as one.

The raw CSV is intentionally ignored by Git. Recreate it with the scraper or provide a compatible private CSV. Anyone collecting fresh data is responsible for reviewing the website's current terms, access rules, and an appropriate request rate.

## Raw schema

| Column | Meaning |
|---|---|
| `title` | Listing title, including year and location text |
| `price` | Advertised PKR price text |
| `year` | Model year from JSON-LD |
| `mileage` | Odometer text |
| `fuel_type` | Parsed engine-spec fuel value |
| `engine_capacity` | Parsed engine-size text |
| `transmission` | Parsed transmission value |
| `link` | Listing URL and deduplication key |

## Verified dataset audit

The tracked report was generated from a 59,668-row CSV with SHA-256:

```text
231376ef4fe5eb192e469ddbb782cc785935585316d77e9085aca6973eb370ea
```

| Cleaning stage | Rows affected |
|---|---:|
| Raw rows | 59,668 |
| Missing title/price/year/link removed | 1,192 |
| Missing/unsupported fuel or kWh rows removed | 1,117 |
| Repeated listing URLs removed | 9,071 |
| Remaining invalid/missing/out-of-range rows removed | 31 |
| Final modeling rows | 48,257 |

The filtering order matters: powertrain exclusions happen before URL deduplication, and deduplication happens before final feature validation.

## Derived fields and validation

- `city` is extracted from the suffix `for sale in …`.
- The location suffix and four-digit year are removed from `title`.
- PKR, commas, `km`, and `cc` are stripped before numeric conversion.
- Supported fuels are Petrol, Diesel, CNG, Hybrid, LPG, and PHEV.
- Supported transmissions are Automatic and Manual.
- Year must be 1950 through the current year plus one.
- Mileage must be 0–2,000,000 km.
- Engine capacity must be 300–10,000 cc.
- Price must be positive.

## Known limitations and risks

- There is no `scraped_at` or listing-update timestamp in the historical CSV.
- Asking price can differ substantially from final sale price.
- Missing condition, trim, registration, assembly origin, and seller attributes create irreducible ambiguity.
- A URL is not guaranteed to identify the same physical car forever, and near-duplicates can use different URLs.
- Marketplace availability and advertiser behavior introduce geographic, socioeconomic, and selection bias.
- Very old, unusual, electric, and incomplete listings are outside the current model's supported scope.
- Prices drift with inflation, exchange rates, taxes, import rules, and supply; this snapshot requires periodic replacement.
