"""HTML and JSON views for car-price inference."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from django.http import HttpRequest, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from .forms import PredictionForm
from .services import (
    ModelPredictionError,
    ModelUnavailableError,
    get_model_metadata,
    predict_price,
)

LOGGER = logging.getLogger(__name__)
MAX_JSON_BYTES = 16 * 1024


@require_GET
def index(request: HttpRequest):
    context = {
        "maximum_year": datetime.now(UTC).year + 1,
        "model_available": False,
        "model_titles": [],
        "model_cities": [],
    }
    try:
        metadata = get_model_metadata()
        categories = metadata.get("known_categories") or {}
        context.update(
            {
                "model_available": True,
                "model_titles": sorted(categories.get("title", [])),
                "model_cities": sorted(categories.get("city", [])),
                "model_trained_at": metadata.get("trained_at_utc"),
            }
        )
    except ModelUnavailableError:
        pass
    return render(request, "predictor/index.html", context)


def _form_errors(form: PredictionForm) -> dict[str, list[str]]:
    return {
        field: [error["message"] for error in errors]
        for field, errors in form.errors.get_json_data(escape_html=True).items()
    }


def predict(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"status": "error", "error": "Only POST is allowed."}, status=405)
    if request.content_type != "application/json":
        return JsonResponse(
            {"status": "error", "error": "Content-Type must be application/json."}, status=415
        )
    content_length = request.META.get("CONTENT_LENGTH")
    try:
        request_size = int(content_length) if content_length else 0
    except ValueError:
        return JsonResponse({"status": "error", "error": "Invalid Content-Length."}, status=400)
    if request_size > MAX_JSON_BYTES:
        return JsonResponse({"status": "error", "error": "Request body is too large."}, status=413)
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse(
            {"status": "error", "error": "Request body is not valid JSON."}, status=400
        )
    if not isinstance(payload, dict):
        return JsonResponse({"status": "error", "error": "A JSON object is required."}, status=400)

    form = PredictionForm(payload)
    if not form.is_valid():
        return JsonResponse(
            {
                "status": "error",
                "error": "Please correct the highlighted input values.",
                "fields": _form_errors(form),
            },
            status=400,
        )

    try:
        result = predict_price(form.cleaned_data)
    except ModelUnavailableError as exc:
        LOGGER.warning("Prediction requested without a usable model: %s", exc)
        return JsonResponse(
            {
                "status": "error",
                "error": "The prediction model is not available. Train the model and try again.",
            },
            status=503,
        )
    except ModelPredictionError:
        LOGGER.exception("Prediction pipeline failed")
        return JsonResponse(
            {"status": "error", "error": "The prediction could not be completed."}, status=500
        )

    return JsonResponse(
        {
            "status": "success",
            "price": result.price,
            "formatted_price": f"PKR {result.price:,.0f}",
            "warnings": result.warnings,
        }
    )
