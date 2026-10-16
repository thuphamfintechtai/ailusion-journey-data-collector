"""
Cao Google Maps qua REST API cua gosom/google-maps-scraper (service gmaps-scraper).

Chay scraper o che do web (-web) thanh mot service rieng, DAG chi goi HTTP:
POST /api/v1/jobs -> poll GET /api/v1/jobs/{id} toi khi Status = ok -> tai CSV.
Khong goi `docker run` tu worker de khoi phai mount Docker socket vao Airflow.

Do that 2026-10-06 (IP Viet Nam, khong dang nhap Google):
- Job 2 tu khoa xong trong ~10 giay. Moi dong tu khoa gan ID rieng bang cu phap
  "<tu khoa>#!#<id>"; CSV tra lai dung ID do o cot input_id nen ghep duoc ket qua.
- Co: gio mo cua 7 ngay, rating, tong so review, phan bo sao, thuoc tinh tiep can,
  mo ta, 5-8 review mau. KHONG co: popular_times (luon rong), price_range,
  thuoc tinh tre em / mon chay.
- Cao don dap thi Google tam tra rong (lo 6 tu khoa ra 10/12 rong, chay lai thi co),
  nen DAG chay tung lo nho, nghi giua cac lo va khong ghi ket qua rong.
- Tu khoa theo ten tra nhieu diem cung ten ("Cho Da Lat" ra ca chuc quay hang), nen
  danh dau best_match bang do khop ten (Jaccard).

Du an di thi, khong thuong mai. Cao Google Maps trai dieu khoan Google.
"""

import csv
import io
import json
import logging
import re
import time
import unicodedata

import requests

logger = logging.getLogger(__name__)

ID_SEP = "#!#"
REVIEW_MAX_CHARS = 1000

# Cac cot CSV la chuoi JSON
JSON_COLUMNS = {
    "open_hours", "popular_times", "reviews_per_rating", "about", "user_reviews",
    "user_reviews_extended", "complete_address", "owner", "images", "menu",
}


class ScraperError(Exception):
    pass


def strip_accents(s: str) -> str:
    s = s.replace("đ", "d").replace("Đ", "D")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def name_score(query: str, title: str) -> float:
    """Jaccard tren tu cua ten. Dung Jaccard thay vi dem tu cua truy van: "Quay dac san
    cho Da Lat" chua du tu cua "Cho Da Lat" nhung thua nhieu tu, phai xep sau cai cho."""
    a = set(strip_accents(query.split(",")[0]).split())
    b = set(strip_accents(title or "").split())
    return len(a & b) / max(len(a | b), 1)


class GmapsScraperClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def create_job(self, name: str, keywords: list[tuple[str, str]], lang: str = "vi", depth: int = 1,
                   max_time: int = 600, extra_reviews: bool = False) -> str:
        """keywords: [(id, tu khoa)]. Tra ve id job."""
        body = {
            "name": name,
            "keywords": [f"{q}{ID_SEP}{k}" for k, q in keywords],
            "lang": lang,
            "zoom": 15,
            "depth": depth,
            "max_time": max_time,
            # Khong co trong spec.yaml nhung server nhan (thay o GET /api/v1/jobs)
            "extra_reviews": extra_reviews,
        }
        r = self.session.post(f"{self.base_url}/api/v1/jobs", json=body, timeout=self.timeout)
        if r.status_code not in (200, 201):
            raise ScraperError(f"Tao job that bai HTTP {r.status_code}: {r.text[:200]}")
        return r.json()["id"]

    def job_status(self, job_id: str) -> str:
        r = self.session.get(f"{self.base_url}/api/v1/jobs/{job_id}", timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        # Server tra khoa viet hoa (ID, Status...) khac voi spec.yaml (id, status)
        return str(data.get("Status") or data.get("status") or "").lower()

    def wait_job(self, job_id: str, timeout_seconds: int, poll_seconds: int = 10) -> str:
        han = time.monotonic() + timeout_seconds
        while time.monotonic() < han:
            try:
                st = self.job_status(job_id)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    # Job bi xoa (scraper khoi dong lai / don tay): cho tiep vo ich
                    raise ScraperError(f"Job {job_id} khong con tren scraper") from e
                logger.warning(f"Chua hoi duoc trang thai job {job_id}, se hoi lai: {e}")
                time.sleep(poll_seconds)
                continue
            except requests.RequestException as e:
                # Scraper dang cao (CPU cao) co luc tra loi API cham qua timeout; job van
                # chay binh thuong nen chi bo qua lan hoi nay, khong bo ca lo (gap 07/10/2026)
                logger.warning(f"Chua hoi duoc trang thai job {job_id}, se hoi lai: {e}")
                time.sleep(poll_seconds)
                continue
            if st in ("ok", "failed"):
                return st
            time.sleep(poll_seconds)
        raise ScraperError(f"Job {job_id} chua xong sau {timeout_seconds} giay")

    def download_rows(self, job_id: str) -> list[dict]:
        r = self.session.get(f"{self.base_url}/api/v1/jobs/{job_id}/download", timeout=self.timeout * 4)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return parse_csv(r.content.decode("utf-8-sig"))

    def list_jobs(self) -> list[dict]:
        r = self.session.get(f"{self.base_url}/api/v1/jobs", timeout=self.timeout)
        r.raise_for_status()
        return r.json() or []

    def clear_orphan_jobs(self, prefix: str = "airflow-", busy_timeout: int = 900, poll_seconds: int = 15) -> None:
        """
        Don job airflow-* con sot tren scraper truoc khi tao job moi.

        DAG chi chay mot lo mot luc, nen job airflow-* con pending / working luc nay la
        do lan thu truoc bi timeout bo lai. Scraper chay job tuan tu: de lai thi job moi
        phai xep hang sau, timeout tiep va don thanh chuoi (07-08/10/2026: 21 job pending
        sau mot job treo). Xoa job working khong dung duoc no (do 08/10/2026), nen chi
        cho no xong; qua busy_timeout thi coi nhu scraper treo, phai khoi dong lai.
        """
        han = time.monotonic() + busy_timeout
        while True:
            jobs = [j for j in self.list_jobs() if str(j.get("Name") or "").startswith(prefix)]
            pending = [j for j in jobs if str(j.get("Status")).lower() == "pending"]
            working = [j for j in jobs if str(j.get("Status")).lower() == "working"]
            for j in pending:
                logger.warning(f"Xoa job pending con sot {j['ID']} ({j.get('Name')})")
                self.delete_job(j["ID"])
            if not working:
                return
            if time.monotonic() > han:
                # Sau khi scraper khoi dong lai, job dang chay luc treo van giu trang thai
                # working du khong con tien trinh nao chay no (gap 08/10/2026) va chan ca
                # hang doi. Xoa di de lan thu lai sau khong bi chan; neu job that ra van
                # chay thi xoa cung vo hai (xoa khong dung duoc job dang chay).
                for j in working:
                    self.delete_job(j["ID"])
                raise ScraperError(
                    f"Scraper van chay job {working[0]['ID']} ({working[0].get('Name')}) sau {busy_timeout} giay, "
                    "co the dang treo: can docker restart gmaps-scraper"
                )
            logger.warning(f"Cho job con sot {working[0]['ID']} chay xong truoc khi tao job moi")
            time.sleep(poll_seconds)

    def delete_job(self, job_id: str) -> None:
        try:
            self.session.delete(f"{self.base_url}/api/v1/jobs/{job_id}", timeout=self.timeout)
        except requests.RequestException as e:
            logger.warning(f"Xoa job {job_id} that bai: {e}")


def parse_csv(text: str) -> list[dict]:
    csv.field_size_limit(10 ** 9)
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        for col in JSON_COLUMNS & row.keys():
            val = row[col]
            try:
                row[col] = json.loads(val) if val not in ("", "null") else None
            except json.JSONDecodeError:
                row[col] = None
        rows.append(row)
    return rows


def _num(x, kind=float):
    try:
        return kind(x) if x not in (None, "") else None
    except (TypeError, ValueError):
        return None


def normalize(row: dict) -> dict:
    """Mot dong CSV -> ban ghi gon de luu. Bo ten / anh nguoi viet review (du lieu ca nhan)."""
    co, khong = {}, {}
    for nhom in row.get("about") or []:
        for o in nhom.get("options") or []:
            (co if o.get("enabled") else khong).setdefault(nhom.get("name") or nhom.get("id"), []).append(o.get("name"))
    reviews = []
    for rv in (row.get("user_reviews") or []) + (row.get("user_reviews_extended") or []):
        text = rv.get("text_original") or rv.get("Description")
        if text:
            reviews.append({
                "rating": _num(rv.get("rating_float") or rv.get("Rating")),
                "date": (rv.get("published_at") or "")[:10] or rv.get("When"),
                "language": rv.get("language") or None,
                "text": text[:REVIEW_MAX_CHARS],
            })
    rpr = row.get("reviews_per_rating") or {}
    return {
        "cid": row.get("cid") or None,
        "place_id": row.get("place_id") or None,
        "data_id": row.get("data_id") or None,
        "input_id": row.get("input_id"),
        "title": row.get("title"),
        "category": row.get("category") or None,
        "address": row.get("address") or None,
        "lat": _num(row.get("latitude")),
        "lng": _num(row.get("longitude")),
        "rating": _num(row.get("review_rating")),
        "review_count": _num(row.get("review_count"), int),
        "reviews_per_rating": rpr if any(rpr.values()) else None,
        "open_hours": row.get("open_hours") or None,
        "popular_times": row.get("popular_times") or None,
        "price_range": row.get("price_range") or None,
        # Scraper dat cau tom tat cua Google vao cot status
        "summary": row.get("status") or None,
        "description": row.get("descriptions") or None,
        "attributes": co or None,
        "attributes_not_available": khong or None,
        "website": row.get("website") or None,
        "phone": row.get("phone") or None,
        "maps_url": row.get("link") or None,
        "reviews": reviews or None,
        "thumbnail": row.get("thumbnail") or None,
        "images": _image_urls(row.get("images")),
    }


IMAGES_MAX = 6


def _image_urls(images) -> list[str] | None:
    """Cot images cua scraper: [{"title": "Tat ca", "image": url}, ...] -> toi da IMAGES_MAX url."""
    urls = []
    for img in images or []:
        url = img.get("image") if isinstance(img, dict) else None
        if url and url not in urls:
            urls.append(url)
    return urls[:IMAGES_MAX] or None


# Truy van loai hinh can cuon danh sach ket qua. Do 07/10/2026: depth 10 ra 60-70 dia
# diem / truy van, ~95 giay / truy van; depth 1 chi ~16.
CATEGORY_DEPTH = 10


def scrape_batch(client: GmapsScraperClient, batch: list[dict], job_timeout: int | None = None,
                 extra_reviews: bool = False) -> tuple[list[dict], list[str]]:
    """
    Cao mot lo truy van [{"id", "query", "destination", "kind"?}]. Mot lo chi nen gom
    mot kind: depth va thoi gian cho tinh theo kind cua phan tu dau.

    Returns:
        (ban ghi dia diem da gan seed_query / destination / best_match, cac truy van ra rong)
    """
    theo_id = {b["id"]: b for b in batch}
    if hasattr(client, "clear_orphan_jobs"):
        client.clear_orphan_jobs()
    is_category = batch[0].get("kind") == "category"
    depth = CATEGORY_DEPTH if is_category else 1
    if job_timeout is None:
        job_timeout = 300 * len(batch) if is_category else 900
    job_id = client.create_job(
        name=f"airflow-{batch[0]['destination']}-{int(time.time())}",
        keywords=[(b["id"], b["query"]) for b in batch],
        depth=depth,
        max_time=job_timeout,
        extra_reviews=extra_reviews,
    )
    logger.info(f"Da tao job {job_id} cho {len(batch)} truy van")
    try:
        # Cho lau hon max_time cua server mot chut de nhan duoc ket qua da cao toi han
        status = client.wait_job(job_id, job_timeout + 120)
        if status != "ok":
            raise ScraperError(f"Job {job_id} ket thuc voi trang thai {status}")
        rows = client.download_rows(job_id)
    finally:
        client.delete_job(job_id)

    nhom: dict[str, list[dict]] = {k: [] for k in theo_id}
    for row in rows:
        p = normalize(row)
        if p["input_id"] in nhom and p["cid"]:
            nhom[p["input_id"]].append(p)

    places, rong = [], []
    for k, ds in nhom.items():
        seed = theo_id[k]
        if not ds:
            rong.append(seed["query"])
            continue
        if seed.get("kind") == "category":
            # Truy van loai hinh khong co "dia diem dung" de khop ten: giu tat ca, khong
            # danh best_match (de khong lan voi ket qua khop ten cua truy van landmark)
            places += [{**p, "seed_query": seed["query"], "destination": seed["destination"],
                        "best_match": False} for p in ds]
            continue
        # Hoa diem ten (VD "Dinh Bao Dai" khop ngang Dinh I va Dinh III) thi chon noi nhieu
        # review hon: thuong la dia diem noi tieng ma nguoi dung muon noi toi.
        ung_vien = ds
        if seed.get("address_hint"):
            # Co goi y dia chi thi chi chon trong cac diem co dia chi chua goi y (neu co)
            goi_y = strip_accents(seed["address_hint"])
            ung_vien = [p for p in ds if goi_y in strip_accents(p.get("address") or "")] or ds
        best = max(ung_vien, key=lambda p: (name_score(seed["query"], p["title"]), p.get("review_count") or 0))
        for p in ds:
            places.append({**p, "seed_query": seed["query"], "destination": seed["destination"],
                           "best_match": p is best})
    return places, rong
