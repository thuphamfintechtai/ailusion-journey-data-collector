"""
Ghi thoi tiet Open-Meteo vao database MLAI-AILUSION.

Hai bang:
- weather_current: thoi tiet hien tai, giu lich su (moi lan chay them mot moc
  observed_at) de ve duoc dien bien theo thoi gian.
- weather_forecast: du bao theo ngay, chi giu ban du bao moi nhat cho moi
  ngay — ban cu khong con gia tri khi da co ban moi hon.

Nhan connection DB-API (psycopg2) tu ben ngoai, giong lehoi.db.
"""

import logging

from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_current (
    destination   TEXT NOT NULL,
    observed_at   TIMESTAMPTZ NOT NULL,
    country       TEXT,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    timezone      TEXT,
    temperature   NUMERIC,
    humidity      NUMERIC,
    weather_code  INTEGER,
    weather_desc  TEXT,
    wind_speed    NUMERIC,
    fetched_at    TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (destination, observed_at)
);

CREATE TABLE IF NOT EXISTS weather_forecast (
    destination         TEXT NOT NULL,
    forecast_date       DATE NOT NULL,
    country             TEXT,
    temp_max            NUMERIC,
    temp_min            NUMERIC,
    weather_code        INTEGER,
    weather_desc        TEXT,
    precipitation_prob  NUMERIC,
    fetched_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (destination, forecast_date)
);

CREATE INDEX IF NOT EXISTS weather_forecast_date_idx ON weather_forecast (forecast_date);
"""

CURRENT_COLUMNS = [
    "destination", "observed_at", "country", "lat", "lon", "timezone", "temperature",
    "humidity", "weather_code", "weather_desc", "wind_speed", "fetched_at",
]
FORECAST_COLUMNS = [
    "destination", "forecast_date", "country", "temp_max", "temp_min", "weather_code",
    "weather_desc", "precipitation_prob", "fetched_at",
]

# Open-Meteo cap nhat current moi 15 phut; chay lai trong cung khung thi ghi de
# cung mot dong thay vi bao loi trung khoa.
UPSERT_CURRENT_SQL = f"""
INSERT INTO weather_current ({", ".join(CURRENT_COLUMNS)})
VALUES %s
ON CONFLICT (destination, observed_at) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in CURRENT_COLUMNS[2:])}
"""

UPSERT_FORECAST_SQL = f"""
INSERT INTO weather_forecast ({", ".join(FORECAST_COLUMNS)})
VALUES %s
ON CONFLICT (destination, forecast_date) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in FORECAST_COLUMNS[2:])},
    updated_at = now()
"""


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def upsert_weather(conn, weather_data: list[dict]) -> tuple[int, int]:
    """
    Ghi ket qua fetch_weather cua nhieu diem den.

    Returns:
        (so dong weather_current, so dong weather_forecast) da ghi
    """
    current_rows = {}
    forecast_rows = {}

    for item in weather_data:
        if not item:
            continue
        current = item.get("current") or {}
        if current.get("observed_at"):
            key = (item["destination"], current["observed_at"])
            current_rows[key] = (
                item["destination"], current["observed_at"], item.get("country"),
                item.get("lat"), item.get("lon"), item.get("timezone"),
                current.get("temperature"), current.get("humidity"),
                current.get("weather_code"), current.get("weather_desc"),
                current.get("wind_speed"), item["fetched_at"],
            )
        else:
            logger.warning(f"Bo qua thoi tiet hien tai thieu observed_at: {item['destination']}")

        for day in item.get("forecast") or []:
            key = (item["destination"], day["date"])
            forecast_rows[key] = (
                item["destination"], day["date"], item.get("country"),
                day.get("temp_max"), day.get("temp_min"), day.get("weather_code"),
                day.get("weather_desc"), day.get("precipitation_prob"), item["fetched_at"],
            )

    with conn.cursor() as cur:
        if current_rows:
            execute_values(cur, UPSERT_CURRENT_SQL, list(current_rows.values()))
        if forecast_rows:
            execute_values(cur, UPSERT_FORECAST_SQL, list(forecast_rows.values()))
    conn.commit()

    logger.info(
        f"Da ghi {len(current_rows)} dong weather_current, "
        f"{len(forecast_rows)} dong weather_forecast"
    )
    return len(current_rows), len(forecast_rows)
