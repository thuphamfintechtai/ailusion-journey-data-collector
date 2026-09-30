"""
DAG thu thap thoi tiet tu Open-Meteo.

Chay 4 lan/ngay (moi 6 tieng) de cap nhat du bao cho cac diem den du lich.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Danh sach diem den du lich
DESTINATIONS = [
    {"name": "Ha Noi", "country": "VN", "lat": 21.0285, "lon": 105.8542},
    {"name": "Ho Chi Minh", "country": "VN", "lat": 10.8231, "lon": 106.6297},
    {"name": "Da Nang", "country": "VN", "lat": 16.0544, "lon": 108.2022},
    {"name": "Nha Trang", "country": "VN", "lat": 12.2388, "lon": 109.1967},
    {"name": "Phu Quoc", "country": "VN", "lat": 10.2270, "lon": 103.9670},
    {"name": "Da Lat", "country": "VN", "lat": 11.9404, "lon": 108.4583},
    {"name": "Hoi An", "country": "VN", "lat": 15.8801, "lon": 108.3380},
    {"name": "Sa Pa", "country": "VN", "lat": 22.3364, "lon": 103.8438},
    {"name": "Ha Long", "country": "VN", "lat": 20.9101, "lon": 107.1839},
    {"name": "Hue", "country": "VN", "lat": 16.4637, "lon": 107.5909},
    # Quoc te
    {"name": "Bangkok", "country": "TH", "lat": 13.7563, "lon": 100.5018},
    {"name": "Singapore", "country": "SG", "lat": 1.3521, "lon": 103.8198},
    {"name": "Seoul", "country": "KR", "lat": 37.5665, "lon": 126.9780},
    {"name": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
    {"name": "Bali", "country": "ID", "lat": -8.3405, "lon": 115.0920},
]


@dag(
    dag_id="weather_open_meteo",
    description="Thu thap thoi tiet tu Open-Meteo cho cac diem den du lich",
    schedule="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "open-meteo", "data-collection"],
    default_args={
        "owner": "ailusion",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def weather_open_meteo():

    @task
    def fetch_weather(destination: dict) -> dict:
        from weather.open_meteo import fetch_weather as _fetch
        return _fetch(destination)

    @task
    def save_weather(weather_data: list[dict]) -> str:
        from weather.open_meteo import save_weather_to_file
        return save_weather_to_file(weather_data)

    weather_results = fetch_weather.expand(destination=DESTINATIONS)
    save_weather(weather_results)


weather_open_meteo()
