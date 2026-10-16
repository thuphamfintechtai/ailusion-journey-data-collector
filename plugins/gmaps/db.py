"""
Ghi dia diem Google Maps vao bang gmaps_place trong database MLAI-AILUSION.

Khoa chinh la cid (Customer ID cua Google): luon co, on dinh; place_id doi khi trong.
Mot dia diem co the hien ra o nhieu truy van seed: best_match giu true neu tung la
ket qua khop nhat cua mot truy van nao do.
"""

import json
import logging

from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gmaps_place (
    cid                       TEXT PRIMARY KEY,
    place_id                  TEXT,
    data_id                   TEXT,
    destination               TEXT NOT NULL,
    seed_query                TEXT NOT NULL,
    best_match                BOOLEAN NOT NULL DEFAULT false,
    title                     TEXT NOT NULL,
    category                  TEXT,
    address                   TEXT,
    lat                       DOUBLE PRECISION,
    lng                       DOUBLE PRECISION,
    rating                    NUMERIC,
    review_count              INTEGER,
    reviews_per_rating        JSONB,
    open_hours                JSONB,
    popular_times             JSONB,
    price_range               TEXT,
    summary                   TEXT,
    description               TEXT,
    attributes                JSONB,
    attributes_not_available  JSONB,
    website                   TEXT,
    phone                     TEXT,
    maps_url                  TEXT,
    reviews                   JSONB,
    thumbnail                 TEXT,
    images                    JSONB,
    source                    TEXT NOT NULL DEFAULT 'google_maps_scraper',
    fetched_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Bang tao truoc khi co anh: them cot (chi them, khong dong du lieu cu).
ALTER TABLE gmaps_place ADD COLUMN IF NOT EXISTS thumbnail TEXT;
ALTER TABLE gmaps_place ADD COLUMN IF NOT EXISTS images JSONB;

CREATE INDEX IF NOT EXISTS gmaps_place_destination_idx ON gmaps_place (destination);
CREATE INDEX IF NOT EXISTS gmaps_place_seed_idx ON gmaps_place (seed_query) WHERE best_match;
CREATE INDEX IF NOT EXISTS gmaps_place_place_id_idx ON gmaps_place (place_id);

-- Moi truy van da chay lan cuoi luc nao, ra bao nhieu dia diem. Dung de chon truy van
-- can cao lai (cu nhat truoc) va de truy van ra rong khong bi cao lai moi ngay.
CREATE TABLE IF NOT EXISTS gmaps_query_run (
    query         TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    destination   TEXT NOT NULL,
    area          TEXT,
    category      TEXT,
    result_count  INTEGER NOT NULL,
    last_run_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

COLUMNS = [
    "cid", "place_id", "data_id", "destination", "seed_query", "best_match", "title",
    "category", "address", "lat", "lng", "rating", "review_count", "reviews_per_rating",
    "open_hours", "popular_times", "price_range", "summary", "description", "attributes",
    "attributes_not_available", "website", "phone", "maps_url", "reviews", "thumbnail", "images",
]
JSON_COLUMNS = {
    "reviews_per_rating", "open_hours", "popular_times", "attributes",
    "attributes_not_available", "reviews", "images",
}
UPDATE_COLUMNS = [c for c in COLUMNS if c not in ("cid", "seed_query", "best_match", "destination")]
# Lan cao sau khong co anh thi giu anh cu (anh con duoc bu tu SerpApi khi lap lo trinh).
KEEP_OLD_IF_NULL = {"thumbnail", "images"}

# seed_query / destination chi doi khi ban moi la best_match; best_match giu true neu dang
# khop nhat cho mot truy van khac (cung mot cho co the la ket qua phu cua truy van nay).
# Co cu cua chinh truy van dang ghi da duoc bo truoc khi upsert, xem upsert_places.
UPSERT_SQL = f"""
INSERT INTO gmaps_place ({", ".join(COLUMNS)}, fetched_at)
VALUES %s
ON CONFLICT (cid) DO UPDATE SET
    {", ".join(
        f"{c} = COALESCE(EXCLUDED.{c}, gmaps_place.{c})" if c in KEEP_OLD_IF_NULL else f"{c} = EXCLUDED.{c}"
        for c in UPDATE_COLUMNS
    )},
    seed_query = CASE WHEN EXCLUDED.best_match THEN EXCLUDED.seed_query ELSE gmaps_place.seed_query END,
    destination = CASE WHEN EXCLUDED.best_match THEN EXCLUDED.destination ELSE gmaps_place.destination END,
    best_match = gmaps_place.best_match OR EXCLUDED.best_match,
    fetched_at = now(),
    updated_at = now()
"""


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def _to_row(p: dict) -> tuple:
    vals = []
    for c in COLUMNS:
        v = p.get(c)
        if c in JSON_COLUMNS:
            v = json.dumps(v, ensure_ascii=False) if v is not None else None
        vals.append(v)
    return tuple(vals)


def upsert_places(conn, places: list[dict]) -> int:
    rows: dict[str, dict] = {}
    for p in places:
        if not p.get("cid") or not p.get("title"):
            continue
        # Cung cid hai lan trong mot lo: giu ban la best_match
        if p["cid"] not in rows or p.get("best_match"):
            rows[p["cid"]] = p
    if not rows:
        return 0
    template = "(" + ", ".join(["%s"] * len(COLUMNS)) + ", now())"
    # Truy van vua co ket qua khop nhat moi: bo co best_match cu cua chinh truy van do, neu
    # khong lan cao truoc chon nham (VD Dinh I thay vi Dinh III) se dinh mai.
    best_moi = {p["seed_query"]: p["cid"] for p in rows.values() if p.get("best_match")}
    with conn.cursor() as cur:
        if best_moi:
            cur.execute(
                "UPDATE gmaps_place SET best_match = false WHERE seed_query = ANY(%s) AND NOT (cid = ANY(%s))",
                (list(best_moi), list(best_moi.values())),
            )
        execute_values(cur, UPSERT_SQL, [_to_row(p) for p in rows.values()], template=template, page_size=200)
    conn.commit()
    logger.info(f"Da ghi {len(rows)} dia diem vao bang gmaps_place")
    return len(rows)


def fresh_queries(conn, max_age_days: int) -> set[str]:
    """Cac truy van seed da co ket qua khop nhat trong max_age_days ngay gan day."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT seed_query FROM gmaps_place WHERE best_match AND fetched_at > now() - %s * interval '1 day'",
            (max_age_days,),
        )
        return {r[0] for r in cur.fetchall()}


def count_places(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), count(*) FILTER (WHERE best_match) FROM gmaps_place")
        return cur.fetchone()


def record_query_runs(conn, batch: list[dict], places: list[dict]) -> None:
    """Ghi lai cac truy van cua mot lo vua chay, ke ca truy van ra rong."""
    counts: dict[str, int] = {}
    for p in places:
        counts[p["seed_query"]] = counts.get(p["seed_query"], 0) + 1
    rows = [
        (b["query"], b.get("kind", "landmark"), b["destination"], b.get("area"),
         b.get("category"), counts.get(b["query"], 0))
        for b in batch
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO gmaps_query_run (query, kind, destination, area, category, result_count)
            VALUES %s
            ON CONFLICT (query) DO UPDATE SET
                result_count = EXCLUDED.result_count,
                last_run_at = now()
            """,
            rows,
        )
    conn.commit()


def category_last_runs(conn) -> dict[str, tuple]:
    """{truy van loai hinh: (last_run_at, result_count)} cua cac truy van da tung chay."""
    with conn.cursor() as cur:
        cur.execute("SELECT query, last_run_at, result_count FROM gmaps_query_run WHERE kind = 'category'")
        return {q: (ts, n) for q, ts, n in cur.fetchall()}
