# Model card

## Model details

- **Task:** regression of Pakistani used-car advertised price in PKR
- **Pipeline:** one-hot categorical features plus numeric passthrough
- **Estimator:** XGBoost regressor with 500 trees
- **Target transformation:** `log1p` during training, `expm1` during inference
- **Categorical features:** title, fuel type, city, transmission
- **Numeric features:** mileage, engine capacity, year
- **Split:** one 80/20 `GroupShuffleSplit`, random state 42
- **Grouping:** stable hash of all seven model inputs
- **Training device:** CPU

No hyperparameter search was performed against the holdout set. The parameters are fixed in code, and the holdout is used once for the reported evaluation.

## Verified evaluation

Generated August 4, 2026 using 38,583 training rows and 9,674 holdout rows. The raw dataset hash and exact dependency versions are recorded in [the JSON report](../reports/model_metrics.json).

| Metric | Model | Median baseline |
|---|---:|---:|
| MAE | PKR 496,914.77 | PKR 2,820,724.45 |
| Median absolute error | PKR 203,541.59 | PKR 1,650,000.00 |
| RMSE | PKR 2,778,619.63 | PKR 6,999,046.96 |
| R² | 0.83524 | -0.04537 |
| MAPE | 11.5368% | 99.3115% |
| Within 10% | 60.7401% | 9.7168% |
| Within 20% | 86.1071% | 21.4286% |

Feature-group overlap between training and holdout is exactly zero.

## Performance by advertised-price band

| Holdout band | Rows | MAE | MAPE | R² |
|---|---:|---:|---:|---:|
| Below PKR 1m | 1,274 | PKR 124,058 | 21.34% | -0.0769 |
| PKR 1m–3m | 3,504 | PKR 216,816 | 11.41% | 0.7110 |
| PKR 3m–7m | 3,635 | PKR 385,410 | 8.60% | 0.7380 |
| PKR 7m–15m | 945 | PKR 869,622 | 9.20% | 0.5942 |
| PKR 15m+ | 316 | PKR 5,274,119 | 14.21% | 0.5079 |

The aggregate R² should not obscure the negative R² below PKR 1 million or the PKR 15.0 million luxury-segment RMSE. Those segments require additional features, more representative data, and potentially specialized models.

## Appropriate use

- Educational demonstration of an end-to-end ML system
- Approximate asking-price reference for in-distribution listings
- Baseline for further feature and data-quality work

## Inappropriate use

- Formal vehicle appraisal, lending, insurance, taxation, or legal decisions
- Claiming the output is a completed-sale price
- Assuming an unseen model/city has the same reliability as trained categories
- Using old artifacts as a live market feed

## Limitations

- The historical data has no timestamp suitable for temporal validation.
- URL and exact-feature leakage are controlled, but near-duplicate listings may remain.
- Important condition, trim, registration, seller, and negotiation variables are missing.
- One holdout split does not quantify variability across time or alternative partitions.
- Model calibration and prediction intervals are not implemented.
- Market drift can quickly invalidate nominal PKR estimates.

## Reproducibility

```bash
python -m car_price_ml.train \
  --data data/raw/pakwheels_cars.csv \
  --output artifacts/car_price_pipeline.joblib \
  --metrics reports/model_metrics.json \
  --random-state 42 \
  --n-estimators 500
```

The training report records row counts, configuration, environment versions, dataset SHA-256, and artifact SHA-256.
