"""
DAG cao thong tin dia diem tu Google Maps (gio mo cua, rating, tiep can, review mau).

Goi service gmaps-scraper (gosom/google-maps-scraper o che do web, xem
docker-compose.gmaps.yaml). Hai loai truy van:
- landmark: ten dia diem cu the, lay tu plugins/gmaps/seeds/<diem_den>.txt. Bo qua neu
  da co ket qua khop nhat trong max_age_days ngay.
- category: "cac <loai hinh> tot nhat o <khu vuc>" cho 34 tinh/thanh, lay tu
  plugins/gmaps/seeds/category/ (~1.070 truy van, moi truy van 60-70 dia diem).
  Moi lan chay chi lay max_category_queries truy van chua cao hoac cu nhat, de rai
  tai: luot dau mat ~1 tuan, sau do moi truy van duoc lam moi ~category_max_age_days.

Chay 02:00 hang ngay (gio Viet Nam), mot lo mot luc va nghi giua cac lo de giam rui ro
bi Google chan. force=true thi cao lai ca truy van con moi.

Ket qua upsert theo cid vao bang gmaps_place cua database MLAI-AILUSION.
"""

import logging
import os
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.sdk import Param, Variable, get_current_context

DB_CONN_ID = "mlai_ailusion"
# Trong cum compose (khi gop docker-compose.gmaps.yaml) goi theo ten service. Chay
# scraper rieng ngoai compose thi dat Variable gmaps_scraper_url, VD
# http://host.docker.internal:8090
DEFAULT_SCRAPER_URL = os.getenv("GMAPS_SCRAPER_URL", "http://gmaps-scraper:8080")

log = logging.getLogger(__name__)


@dag(
    dag_id="gmaps_places",
    description="Cao thong tin dia diem tu Google Maps qua gosom/google-maps-scraper",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["google-maps", "poi", "data-collection"],
    default_args={
        "owner": "ailusion",
        "retries": 2,
        # Lo bi rong het thuong la Google tam chan: doi lau roi moi thu lai
        "retry_delay": timedelta(minutes=15),
    },
    params={
        "destinations": Param(
            ["dalat", "danang", "hanoi"], type="array", description="Khoa diem den, trung ten file trong plugins/gmaps/seeds"
        ),
        "batch_size": Param(4, type="integer", minimum=1, maximum=20, description="So truy van moi job scraper"),
        "pause_seconds": Param(30, type="integer", minimum=0, description="Nghi giua cac lo de do bi chan"),
        "max_age_days": Param(7, type="integer", minimum=0, description="Bo qua truy van landmark da cao trong so ngay nay"),
        "include_categories": Param(True, type="boolean", description="Cao them truy van theo loai hinh x khu vuc"),
        "provinces": Param(
            [], type="array", description="Khoa tinh cho truy van loai hinh (cot dau seeds/category/areas.txt), bo trong = 34 tinh"
        ),
        "category_max_age_days": Param(30, type="integer", minimum=0, description="Bo qua truy van loai hinh da cao trong so ngay nay"),
        "empty_retry_days": Param(
            1, type="integer", minimum=0, description="Truy van loai hinh ra rong thi cao lai sau so ngay nay (thuong do Google tam chan)"
        ),
        "max_category_queries": Param(
            150, type="integer", minimum=0, description="So truy van loai hinh toi da moi lan chay (~95 giay / truy van)"
        ),
        "force": Param(False, type="boolean", description="Cao lai ca truy van con moi"),
        "only_queries": Param(
            [], type="array", description="Chi cao cac truy van nay (khop nguyen van dong trong file seed), bo trong = tat ca"
        ),
        "extra_reviews": Param(False, type="boolean", description="Thu lay them review (thuong van chi ~8 review)"),
        "scraper_url": Param("", type="string", description="Bo trong = Variable gmaps_scraper_url hoac mac dinh"),
    },
)
def gmaps_places():

    @task
    def create_table() -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from gmaps.db import ensure_table

        with PostgresHook(postgres_conn_id=DB_CONN_ID).get_conn() as conn:
            ensure_table(conn)

    @task
    def plan() -> list[list[dict]]:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from gmaps.db import category_last_runs, fresh_queries
        from gmaps.seeds import load_category_seeds, load_seeds

        p = get_current_context()["params"]
        seeds = load_seeds(p["destinations"])
        categories = load_category_seeds(p["provinces"] or None) if p["include_categories"] else []
        if p["only_queries"]:
            chi = set(p["only_queries"])
            seeds = [s for s in seeds if s["query"] in chi]
            categories = [s for s in categories if s["query"] in chi]

        with PostgresHook(postgres_conn_id=DB_CONN_ID).get_conn() as conn:
            con_moi = set() if p["force"] else fresh_queries(conn, p["max_age_days"])
            lan_cuoi = category_last_runs(conn)
        seeds = [s for s in seeds if s["query"] not in con_moi]

        if categories:
            now = pendulum.now("UTC")
            moc = now.subtract(days=p["category_max_age_days"])
            # Do 07/10/2026: 12/150 truy van ra rong du chac chan co ket qua ("cac nha hang
            # tot nhat o TP HCM") — Google tam chan. Rong thi thu lai som, khong doi 30 ngay.
            moc_rong = now.subtract(days=p["empty_retry_days"])

            def can_cao(s):
                if s["query"] not in lan_cuoi:
                    return True
                ts, n = lan_cuoi[s["query"]]
                return ts < (moc_rong if n == 0 else moc)

            if not p["force"]:
                categories = [s for s in categories if can_cao(s)]
            # Chua chay bao gio truoc, roi toi cu nhat; giu thu tu file trong cung muc de
            # cac truy van cung khu vuc nam canh nhau
            categories.sort(key=lambda s: lan_cuoi[s["query"]][0] if s["query"] in lan_cuoi else pendulum.datetime(1970, 1, 1))
            con_lai = len(categories)
            categories = categories[:p["max_category_queries"]]
            log.info(f"Truy van loai hinh: {con_lai} can cao, lan nay lay {len(categories)}")

        log.info(f"Can cao {len(seeds)} truy van landmark, {len(categories)} truy van loai hinh")
        # Moi lo chi mot diem den / tinh va mot loai truy van: ten job de doc, va scraper
        # dat depth theo loai truy van cua ca lo
        lo: list[list[dict]] = []
        for dest in p["destinations"]:
            ds = [s for s in seeds if s["destination"] == dest]
            lo += [ds[i:i + p["batch_size"]] for i in range(0, len(ds), p["batch_size"])]
        for province in dict.fromkeys(s["destination"] for s in categories):
            ds = [s for s in categories if s["destination"] == province]
            lo += [ds[i:i + p["batch_size"]] for i in range(0, len(ds), p["batch_size"])]
        return lo

    # Tung lo mot: scraper la mot trinh duyet, chay song song chi them de bi Google chan
    @task(max_active_tis_per_dagrun=1)
    def scrape(batch: list[dict]) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from gmaps.db import record_query_runs, upsert_places
        from gmaps.scraper import GmapsScraperClient, scrape_batch

        p = get_current_context()["params"]
        url = p["scraper_url"] or Variable.get("gmaps_scraper_url", default=DEFAULT_SCRAPER_URL)
        places, rong = scrape_batch(GmapsScraperClient(url), batch, extra_reviews=p["extra_reviews"])
        if len(rong) == len(batch):
            # Ca lo rong: gan nhu chac chan bi chan tam thoi, de Airflow retry sau retry_delay
            raise AirflowException(f"Ca lo {len(batch)} truy van ra rong, co the Google dang chan: {rong}")
        with PostgresHook(postgres_conn_id=DB_CONN_ID).get_conn() as conn:
            written = upsert_places(conn, places)
            record_query_runs(conn, batch, places)
        if rong:
            log.warning(f"{len(rong)} truy van ra rong, se cao lai o lan chay sau: {rong}")
        time.sleep(p["pause_seconds"])
        return {"queries": len(batch), "places": written, "empty": rong}

    @task(trigger_rule="all_done")
    def summary(results: list[dict]) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from gmaps.db import count_places

        ok = [r for r in results if r]
        rong = [q for r in ok for q in r["empty"]]
        with PostgresHook(postgres_conn_id=DB_CONN_ID).get_conn() as conn:
            tong, khop = count_places(conn)
        log.info(
            f"Cao {sum(r['queries'] for r in ok)} truy van, ghi {sum(r['places'] for r in ok)} dia diem; "
            f"bang gmaps_place hien co {tong} dia diem ({khop} khop truy van seed); {len(rong)} truy van rong"
        )

    batches = plan()
    create_table() >> batches
    summary(scrape.expand(batch=batches))


gmaps_places()
