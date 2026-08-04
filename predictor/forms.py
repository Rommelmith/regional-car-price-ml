"""Server-side validation for prediction requests."""

from datetime import UTC, datetime

from django import forms

from car_price_ml.constants import (
    MAX_ENGINE_CAPACITY,
    MAX_MILEAGE,
    MAX_YEAR_OFFSET,
    MIN_ENGINE_CAPACITY,
    MIN_MILEAGE,
    MIN_YEAR,
    SUPPORTED_FUEL_TYPES,
    SUPPORTED_TRANSMISSIONS,
)


class PredictionForm(forms.Form):
    title = forms.CharField(min_length=2, max_length=120, strip=True)
    year = forms.IntegerField(
        min_value=MIN_YEAR,
        max_value=datetime.now(UTC).year + MAX_YEAR_OFFSET,
    )
    mileage = forms.FloatField(min_value=MIN_MILEAGE, max_value=MAX_MILEAGE)
    engine_capacity = forms.FloatField(
        min_value=MIN_ENGINE_CAPACITY,
        max_value=MAX_ENGINE_CAPACITY,
    )
    city = forms.CharField(min_length=2, max_length=80, strip=True)
    transmission = forms.ChoiceField(choices=[(value, value) for value in SUPPORTED_TRANSMISSIONS])
    fuel_type = forms.ChoiceField(choices=[(value, value) for value in SUPPORTED_FUEL_TYPES])

    def clean_title(self) -> str:
        return " ".join(self.cleaned_data["title"].split())

    def clean_city(self) -> str:
        return " ".join(self.cleaned_data["city"].split())
