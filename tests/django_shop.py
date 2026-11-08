"""URLconf for the Django case in test_frameworks."""

import asyncio

from django.http import JsonResponse
from django.urls import path

stock = {"widget": 1}


async def buy(request):
    n = stock["widget"]
    await asyncio.sleep(0.01)
    stock["widget"] = n - 1
    return JsonResponse({"left": stock["widget"]})


urlpatterns = [path("buy", buy)]
