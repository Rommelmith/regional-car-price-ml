"""HTTP-level tests for the prediction application."""

import json
from unittest.mock import patch

from django.test import Client, TestCase
from django.urls import reverse

from .services import ModelPredictionError, ModelUnavailableError, PredictionResult


class PredictionViewTests(TestCase):
    valid_payload = {
        "title": "Honda Civic",
        "year": 2020,
        "mileage": 65_000,
        "engine_capacity": 1800,
        "city": "Lahore",
        "transmission": "Automatic",
        "fuel_type": "Petrol",
    }

    @patch("predictor.views.get_model_metadata", return_value={})
    def test_index_renders_without_requiring_a_model(self, _metadata):
        response = self.client.get(reverse("predictor:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Estimate a regional asking price")

    def test_predict_rejects_get(self):
        response = self.client.get(reverse("predictor:predict"))
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.json()["status"], "error")

    def test_predict_requires_json(self):
        response = self.client.post(reverse("predictor:predict"), data=self.valid_payload)
        self.assertEqual(response.status_code, 415)

    def test_predict_rejects_invalid_json(self):
        response = self.client.post(
            reverse("predictor:predict"), data="{", content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "Request body is not valid JSON.")

    def test_predict_returns_field_validation_errors(self):
        payload = {**self.valid_payload, "year": 1800, "fuel_type": "Steam"}
        response = self.client.post(
            reverse("predictor:predict"),
            data=json.dumps(payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("year", response.json()["fields"])
        self.assertIn("fuel_type", response.json()["fields"])

    @patch(
        "predictor.views.predict_price",
        return_value=PredictionResult(price=4_250_000.4, warnings=[]),
    )
    def test_predict_returns_formatted_prediction(self, mock_predict):
        response = self.client.post(
            reverse("predictor:predict"),
            data=json.dumps(self.valid_payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["formatted_price"], "PKR 4,250,000")
        mock_predict.assert_called_once()

    @patch("predictor.views.predict_price", side_effect=ModelUnavailableError("private path"))
    def test_unavailable_model_returns_safe_503(self, _predict):
        response = self.client.post(
            reverse("predictor:predict"),
            data=json.dumps(self.valid_payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 503)
        self.assertNotContains(response, "private path", status_code=503)

    @patch("predictor.views.predict_price", side_effect=ModelPredictionError("private detail"))
    def test_prediction_failure_returns_safe_500(self, _predict):
        response = self.client.post(
            reverse("predictor:predict"),
            data=json.dumps(self.valid_payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 500)
        self.assertNotContains(response, "private detail", status_code=500)

    @patch("predictor.views.get_model_metadata", return_value={})
    def test_csrf_protection_is_enabled(self, _metadata):
        csrf_client = Client(enforce_csrf_checks=True)
        response = csrf_client.post(
            reverse("predictor:predict"),
            data=json.dumps(self.valid_payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 403)
