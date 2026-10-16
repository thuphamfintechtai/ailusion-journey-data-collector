"""
DAG thu thap lich le hoi, su kien tu lehoivietnam.com.vn.

Chay moi ngay luc 03:00 gio Viet Nam:
- Ngay thuong: delta, chi cao cac su kien moi cap nhat (vai chuc trang dau
  cua tab "Moi nhat").
- Chu nhat, khi bang event con trong, hoac trigger voi full_refresh=true: cao lai
  toan bo (~3.500 trang, mat khoang 15-20 phut) de bat cac thay doi delta bo sot.

Ket qua upsert theo event_id vao bang event cua database MLAI-AILUSION
(Postgres ailusion.io.vn).
"""

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.sdk import Param, get_current_context

# Moi batch cao tuan tu, nghi 1s giua cac trang. Gioi han so batch chay cung
# luc de tong tai len site chi vai request/giay.
BATCH_SIZE = 100
MAX_PARALLEL_BATCHES = 4
REQUEST_DELAY_SECONDS = 1.0

# Database MLAI-AILUSION tren ailusion.io.vn, khai bao qua AIRFLOW_CONN_MLAI_AILUSION trong .env.
# Giu ten cu vi gmaps_places dung chung connection nay.
DB_CONN_ID = "mlai_ailusion"


@dag(
    dag_id="lehoi_vietnam",
    description="Thu thap lich le hoi, su kien tu lehoivietnam.com.vn",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["festival", "lehoivietnam", "data-collection"],
    default_args={
        "owner": "ailusion",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    params={
        "full_refresh": Param(False, type="boolean", description="Cao lai toan bo thay vi delta"),
        "max_pages": Param(
            30,
            type="integer",
            minimum=1,
            description="So trang tab Moi nhat doc o che do delta (~12 su kien/trang)",
        ),
    },
)
def lehoi_vietnam():

    @task
    def decide_mode() -> bool:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from lehoi.db import count_events

        context = get_current_context()
        if context["params"]["full_refresh"]:
            return True
        # Bang trong (DB moi, hoac vua xoa) thi chua co du lieu nen, phai cao
        # het. Khong dua vao prev_data_interval_start_success: lan chay tay
        # khong co data interval nen gia tri do luon rong du da chay thanh cong.
        with PostgresHook(postgres_conn_id=DB_CONN_ID).get_conn() as conn:
            if count_events(conn) == 0:
                return True
        # Lan chay tay trong Airflow 3 co the khong co logical_date, nen lay gio
        # hien tai; lan chay theo lich luon roi vao 03:00 cung ngay.
        return pendulum.now("Asia/Ho_Chi_Minh").day_of_week == pendulum.SUNDAY

    @task
    def discover(full_refresh: bool) -> list[list[dict]]:
        from lehoi.lehoivietnam import discover_event_urls

        context = get_current_context()
        entries = discover_event_urls(full_refresh, context["params"]["max_pages"])
        return [entries[i:i + BATCH_SIZE] for i in range(0, len(entries), BATCH_SIZE)]

    @task(max_active_tis_per_dagrun=MAX_PARALLEL_BATCHES)
    def fetch_batch(entries: list[dict]) -> list[dict]:
        from lehoi.lehoivietnam import fetch_events

        return fetch_events(entries, delay_seconds=REQUEST_DELAY_SECONDS)

    @task
    def create_table() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from lehoi.db import ensure_table

        with PostgresHook(postgres_conn_id=DB_CONN_ID).get_conn() as conn:
            ensure_table(conn)

    @task
    def save(batches: list[list[dict]], url_batches: list[list[dict]]) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from lehoi.db import count_events, upsert_events

        events = [event for batch in batches if batch for event in batch]
        total_urls = sum(len(b) for b in url_batches)
        with PostgresHook(postgres_conn_id=DB_CONN_ID).get_conn() as conn:
            written = upsert_events(conn, events)
            total = count_events(conn)
        logging.getLogger(__name__).info(
            f"Cao {len(events)}/{total_urls} trang, ghi {written} dong, bang hien co {total} su kien"
        )
        return written

    full_refresh = decide_mode()
    create_table() >> full_refresh
    url_batches = discover(full_refresh)
    results = fetch_batch.expand(entries=url_batches)
    save(results, url_batches)


lehoi_vietnam()
