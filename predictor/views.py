from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import pandas as pd
import joblib
import json
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
model_path = os.path.join(BASE_DIR, "car_price_model.pkl")
pipe = joblib.load(model_path)

def index(request):
    return render(request, 'predictor/index.html')

@csrf_exempt
def predict(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)

            car_data = pd.DataFrame([{
                "title": data['title'],
                "year": float(data['year']),
                "mileage": float(data['mileage']),
                "engine_capacity": float(data['engine_capacity']),
                "city": data['city'],
                "transmission": data['transmission'],
                "fuel_type": data['fuel_type'],
            }])

            predicted_price = float(pipe.predict(car_data)[0])

            return JsonResponse({"price": float(predicted_price),
                                 "formatted_price": f"PKR {predicted_price: ,.0f}",
                                "status": "success"})
        except Exception as e:
            return JsonResponse({"error": str(e),
                                 "status": "failed"}
                                , status=400)

    return JsonResponse({"error": "Only POST allowed"}, status=405)