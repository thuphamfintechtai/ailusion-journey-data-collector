"""
Ghi su kien le hoi vao bang event trong database MLAI-AILUSION
(Postgres ailusion.io.vn).

Nhan connection DB-API (psycopg2) tu ben ngoai, de DAG dung PostgresHook con
script chay tay dung psycopg2.connect truc tiep, khong phu thuoc Airflow.
"""

import json
import logging
from datetime import date

from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

TABLE = "event"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    event_id         BIGINT PRIMARY KEY,
    url              TEXT NOT NULL,
    name             TEXT NOT NULL,
    start_date       DATE,
    end_date         DATE,
    date_display     TEXT,
    event_status     TEXT,
    attendance_mode  TEXT,
    location_name    TEXT,
    street_address   TEXT,
    address_region   TEXT,
    address_country  TEXT,
    loc2_id          BIGINT,
    loc3_id          BIGINT,
    admin_areas      JSONB NOT NULL DEFAULT '[]',
    organizer        TEXT,
    price            NUMERIC,
    price_currency   TEXT,
    description      TEXT,
    highlights       JSONB NOT NULL DEFAULT '[]',
    tags             JSONB NOT NULL DEFAULT '[]',
    images           JSONB NOT NULL DEFAULT '[]',
    source           TEXT NOT NULL DEFAULT 'lehoivietnam.com.vn',
    sitemap_lastmod  TIMESTAMPTZ,
    fetched_at       TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS event_start_date_idx ON {TABLE} (start_date);
CREATE INDEX IF NOT EXISTS event_loc2_id_idx ON {TABLE} (loc2_id);
"""

COLUMNS = [
    "event_id", "url", "name", "start_date", "end_date", "date_display",
    "event_status", "attendance_mode", "location_name", "street_address",
    "address_region", "address_country", "loc2_id", "loc3_id", "admin_areas",
    "organizer", "price", "price_currency", "description", "highlights",
    "tags", "images", "sitemap_lastmod", "fetched_at",
]
JSON_COLUMNS = {"admin_areas", "highlights", "tags", "images"}

# created_at giu nguyen tu lan dau thay su kien, chi updated_at doi
UPSERT_SQL = f"""
INSERT INTO {TABLE} ({", ".join(COLUMNS)})
VALUES %s
ON CONFLICT (event_id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS if c != "event_id")},
    updated_at = now()
"""


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def _to_date(value: str | None) -> date | None:
    # JSON-LD thuong la "2026-12-19" nhung schema.org cho phep kem gio
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _to_int(value) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_row(event: dict) -> tuple:
    row = dict(event)
    row["start_date"] = _to_date(event.get("start_date"))
    row["end_date"] = _to_date(event.get("end_date"))
    row["loc2_id"] = _to_int(event.get("loc2_id"))
    row["loc3_id"] = _to_int(event.get("loc3_id"))
    row["price"] = event.get("price") or None
    row["sitemap_lastmod"] = event.get("lastmod")
    for col in JSON_COLUMNS:
        row[col] = json.dumps(event.get(col) or [], ensure_ascii=False)
    return tuple(row.get(c) for c in COLUMNS)


def upsert_events(conn, events: list[dict]) -> int:
    """
    Upsert theo event_id. Chay lai nhieu lan khong tao ban trung.

    Returns:
        so dong da ghi
    """
    rows = {}
    for event in events:
        if not event.get("event_id") or not event.get("name"):
            logger.warning(f"Bo qua su kien thieu id hoac ten: {event.get('url')}")
            continue
        # Cung id xuat hien hai lan (vd. URL cu va moi) thi giu ban sau,
        # vi ON CONFLICT khong cho dung mot khoa hai lan trong cung lenh.
        rows[event["event_id"]] = _to_row(event)

    if not rows:
        logger.info("Khong co su kien nao de ghi")
        return 0

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, list(rows.values()), page_size=500)
    conn.commit()

    logger.info(f"Da ghi {len(rows)} su kien vao bang {TABLE}")
    return len(rows)


def count_events(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {TABLE}")
        return cur.fetchone()[0]

