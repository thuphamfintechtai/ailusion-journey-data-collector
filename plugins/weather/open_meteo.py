"""
Script cao du lieu thoi tiet tu Open-Meteo API.

Open-Meteo mien phi, khong can API key, du lieu tu cac co quan khi tuong chinh thuc.
https://open-meteo.com/en/docs
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# Mapping weather code sang mo ta tieng Viet
# https://open-meteo.com/en/docs#weathervariables
WEATHER_CODES = {
    0: "Troi quang",
    1: "Chu yeu quang",
    2: "Co may",
    3: "U am",
    45: "Suong mu",
    48: "Suong mu dong",
    51: "Mua phun nhe",
    53: "Mua phun",
    55: "Mua phun day",
    61: "Mua nhe",
    63: "Mua vua",
    65: "Mua to",
    71: "Tuyet nhe",
    73: "Tuyet vua",
    75: "Tuyet day",
    80: "Mua rao nhe",
    81: "Mua rao vua",
    82: "Mua rao to",
    95: "Dong",
    96: "Dong kem mua da",
    99: "Dong kem mua da lon",
}


def get_weather_description(code: int) -> str:
    """Chuyen weather code thanh mo ta tieng Viet."""
    return WEATHER_CODES.get(code, f"Ma thoi tiet {code}")


def fetch_weather(destination: dict) -> dict:
    """
    Goi Open-Meteo API de lay thoi tiet cho mot diem den.

    Args:
        destination: dict voi cac key: name, country, lat, lon

    Returns:
        dict chua thoi tiet hien tai va du bao 7 ngay
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": destination["lat"],
        "longitude": destination["lon"],
        "current": "temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
        "timezone": "auto",
        "forecast_days": 7,
    }

    logger.info(f"Dang lay thoi tiet cho {destination['name']}")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Xu ly du lieu hien tai
    current = data.get("current", {})
    current_weather = {
        "temperature": current.get("temperature_2m"),
        "humidity": current.get("relative_humidity_2m"),
        "weather_code": current.get("weather_code"),
        "weather_desc": get_weather_description(current.get("weather_code", -1)),
        "wind_speed": current.get("wind_speed_10m"),
    }

    # Xu ly du bao 7 ngay
    daily = data.get("daily", {})
    forecast = []
    dates = daily.get("time", [])
    for i, date in enumerate(dates):
        forecast.append({
            "date": date,
            "temp_max": daily.get("temperature_2m_max", [None])[i],
            "temp_min": daily.get("temperature_2m_min", [None])[i],
            "weather_code": daily.get("weather_code", [None])[i],
            "weather_desc": get_weather_description(daily.get("weather_code", [-1])[i]),
            "precipitation_prob": daily.get("precipitation_probability_max", [None])[i],
        })

    result = {
        "destination": destination["name"],
        "country": destination["country"],
        "lat": destination["lat"],
        "lon": destination["lon"],
        "fetched_at": datetime.utcnow().isoformat(),
        "timezone": data.get("timezone"),
        "current": current_weather,
        "forecast": forecast,
    }

    logger.info(
        f"{destination['name']}: {current_weather['temperature']}C, "
        f"{current_weather['weather_desc']}"
    )

    return result


def fetch_all_weather(destinations: list[dict]) -> list[dict]:
    """Lay thoi tiet cho tat ca diem den."""
    results = []
    for dest in destinations:
        try:
            weather = fetch_weather(dest)
            results.append(weather)
        except Exception as e:
            logger.error(f"Loi khi lay thoi tiet {dest['name']}: {e}")
            results.append(None)
    return results


def save_weather_to_file(weather_data: list[dict], data_dir: str = "/opt/airflow/data/weather") -> str:
    """
    Luu ket qua vao file JSON.

    Args:
        weather_data: list ket qua tu fetch_weather
        data_dir: thu muc luu file

    Returns:
        duong dan file da luu
    """
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = datetime.utcnow().strftime("%H%M")
    filename = data_path / f"weather_{today}_{timestamp}.json"

    valid_data = [d for d in weather_data if d is not None]

    output = {
        "collected_at": datetime.utcnow().isoformat(),
        "total_destinations": len(valid_data),
        "failed": len(weather_data) - len(valid_data),
        "data": valid_data,
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    logger.info(f"Da luu {len(valid_data)} diem den vao {filename}")

    return str(filename)
