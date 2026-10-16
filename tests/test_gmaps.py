"""
Test plugin gmaps: doc CSV cua scraper, chuan hoa, chon best_match, seed.
Khong goi scraper that, khong can Airflow / Postgres.

Chay: PYTHONPATH=plugins python -m pytest tests/test_gmaps.py -q
"""

import csv
import io
import json

import pytest

from gmaps.scraper import ScraperError, name_score, normalize, parse_csv, scrape_batch
from gmaps.seeds import available_destinations, available_provinces, load_category_seeds, load_seeds

COT = ["input_id", "link", "title", "category", "address", "open_hours", "popular_times", "review_count",
       "review_rating", "reviews_per_rating", "latitude", "longitude", "cid", "status", "descriptions",
       "price_range", "place_id", "about", "user_reviews", "user_reviews_extended", "website", "phone",
       "thumbnail", "images"]


def _csv(rows: list[dict]) -> str:
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=COT)
    w.writeheader()
    for r in rows:
        w.writerow({k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v) for k, v in r.items()})
    return out.getvalue()


DINH = {
    "input_id": "k1", "title": "Dinh III Bảo Đại", "cid": "111", "latitude": "11.93", "longitude": "108.43",
    "review_count": "9648", "review_rating": "4.2", "reviews_per_rating": {"1": 100, "5": 6000},
    "open_hours": {"Thứ Hai": ["07:00–17:30"]}, "popular_times": {}, "price_range": "",
    "status": "Dinh thự thập niên 1930",
    "about": [{"id": "accessibility", "name": "Phù hợp cho người khuyết tật",
               "options": [{"name": "Chỗ đỗ xe cho xe lăn", "enabled": True}, {"name": "Lối vào cho xe lăn", "enabled": False}]}],
    "user_reviews": [{"Name": "Nguyễn Văn A", "rating_float": "4", "text_original": "x" * 1500,
                      "published_at": "2026-06-07T04:05:08Z", "language": "vi"}],
    "user_reviews_extended": "null",
    "thumbnail": "https://lh3.googleusercontent.com/t.jpg",
    "images": [{"title": "Tất cả", "image": f"https://lh3.googleusercontent.com/{i}.jpg"} for i in range(8)]
    + [{"title": "Trùng", "image": "https://lh3.googleusercontent.com/0.jpg"}],
}


def test_parse_va_normalize():
    rows = parse_csv(_csv([DINH]))
    p = normalize(rows[0])
    assert p["cid"] == "111" and p["rating"] == 4.2 and p["review_count"] == 9648 and p["lat"] == 11.93
    assert p["attributes"] == {"Phù hợp cho người khuyết tật": ["Chỗ đỗ xe cho xe lăn"]}
    assert p["attributes_not_available"] == {"Phù hợp cho người khuyết tật": ["Lối vào cho xe lăn"]}
    assert p["popular_times"] is None and p["price_range"] is None
    assert p["reviews"] == [{"rating": 4.0, "date": "2026-06-07", "language": "vi", "text": "x" * 1000}]
    assert "Nguyễn Văn A" not in json.dumps(p, ensure_ascii=False)
    assert p["thumbnail"] == "https://lh3.googleusercontent.com/t.jpg"
    assert p["images"] == [f"https://lh3.googleusercontent.com/{i}.jpg" for i in range(6)]


def test_khong_co_anh_thi_de_trong():
    p = normalize(parse_csv(_csv([{**DINH, "thumbnail": "", "images": "null"}]))[0])
    assert p["thumbnail"] is None and p["images"] is None


def test_upsert_giu_anh_cu_khi_lan_cao_moi_khong_co_anh():
    from gmaps.db import UPSERT_SQL

    assert "thumbnail = COALESCE(EXCLUDED.thumbnail, gmaps_place.thumbnail)" in UPSERT_SQL
    assert "images = COALESCE(EXCLUDED.images, gmaps_place.images)" in UPSERT_SQL


def test_name_score_jaccard():
    q = "Chợ Đà Lạt, Lâm Đồng"
    assert name_score(q, "Chợ Đà Lạt") == 1.0
    assert name_score(q, "Quầy đặc sản chợ Đà Lạt") < name_score(q, "Chợ Đêm Đà Lạt")


class _ClientGia:
    def __init__(self, rows, status="ok"):
        self.rows, self.status, self.da_xoa = rows, status, False

    def create_job(self, **kw):
        self.kw = kw
        return "job1"

    def wait_job(self, job_id, timeout):
        return self.status

    def download_rows(self, job_id):
        return parse_csv(_csv(self.rows))

    def delete_job(self, job_id):
        self.da_xoa = True


def test_scrape_batch_gan_best_match_va_bao_truy_van_rong():
    dinh_i = {**DINH, "title": "Dinh I Bảo Đại", "cid": "222"}
    client = _ClientGia([DINH, dinh_i])
    batch = [
        {"id": "k1", "query": "Dinh III Bảo Đại, Đà Lạt", "destination": "dalat"},
        {"id": "k2", "query": "Nơi không có, Đà Lạt", "destination": "dalat"},
    ]
    places, rong = scrape_batch(client, batch)
    assert rong == ["Nơi không có, Đà Lạt"]
    best = [p for p in places if p["best_match"]]
    assert [p["title"] for p in best] == ["Dinh III Bảo Đại"]
    assert all(p["seed_query"] == "Dinh III Bảo Đại, Đà Lạt" for p in places)
    assert client.kw["keywords"] == [("k1", "Dinh III Bảo Đại, Đà Lạt"), ("k2", "Nơi không có, Đà Lạt")]
    assert client.da_xoa  # job luon duoc xoa de khong dong file tren scraper


def test_hoa_diem_ten_thi_chon_noi_nhieu_review():
    dinh_i = {**DINH, "title": "Dinh I Bảo Đại", "cid": "222", "review_count": "6335"}
    dinh_iii = {**DINH, "title": "Dinh III Bảo Đại", "cid": "333", "review_count": "9648"}
    places, _ = scrape_batch(_ClientGia([dinh_i, dinh_iii]), [{"id": "k1", "query": "Dinh Bảo Đại, Đà Lạt", "destination": "dalat"}])
    assert [p["title"] for p in places if p["best_match"]] == ["Dinh III Bảo Đại"]


def test_goi_y_dia_chi_chon_dung_chi_nhanh():
    chi_nhanh = {**DINH, "title": "Phở Thìn Lò Đúc", "cid": "1", "review_count": "53", "address": "Kim Văn, Hà Nội"}
    quan_goc = {**DINH, "title": "Phở Thìn", "cid": "2", "review_count": "1659", "address": "13 P. Lò Đúc, Hai Bà Trưng"}
    seed = {"id": "k1", "query": "Phở Thìn Lò Đúc, Hà Nội", "address_hint": "13 P. Lò Đúc", "destination": "hanoi"}
    places, _ = scrape_batch(_ClientGia([chi_nhanh, quan_goc]), [seed])
    assert [p["title"] for p in places if p["best_match"]] == ["Phở Thìn"]
    # Khong co goi y thi van chon theo ten nhu cu
    places, _ = scrape_batch(_ClientGia([chi_nhanh, quan_goc]), [{**seed, "address_hint": None}])
    assert [p["title"] for p in places if p["best_match"]] == ["Phở Thìn Lò Đúc"]


def test_scrape_batch_job_failed_van_xoa_job():
    client = _ClientGia([], status="failed")
    with pytest.raises(ScraperError):
        scrape_batch(client, [{"id": "k1", "query": "x", "destination": "dalat"}])
    assert client.da_xoa


def test_seeds():
    assert {"dalat", "danang", "hanoi"} <= set(available_destinations())
    s = load_seeds(["dalat"])
    assert len(s) >= 30 and all(x["destination"] == "dalat" for x in s)
    assert len({x["id"] for x in s}) == len(s)  # id khong trung
    assert not any(x["query"].startswith("#") for x in s)
    with pytest.raises(ValueError):
        load_seeds(["khong_co"])
    pho = next(x for x in load_seeds(["hanoi"]) if x["query"].startswith("Phở Thìn"))
    assert pho["query"] == "Phở Thìn Lò Đúc, Hà Nội" and pho["address_hint"] == "13 P. Lò Đúc"


def test_truy_van_loai_hinh_khong_danh_best_match_va_cuon_sau():
    dinh_i = {**DINH, "title": "Dinh I Bảo Đại", "cid": "222"}
    client = _ClientGia([DINH, dinh_i])
    seed = {"id": "k1", "query": "các điểm tham quan tốt nhất ở Đà Lạt", "destination": "lam-dong", "kind": "category"}
    places, rong = scrape_batch(client, [seed])
    assert rong == []
    assert len(places) == 2
    assert not any(p["best_match"] for p in places)
    assert all(p["destination"] == "lam-dong" for p in places)
    assert client.kw["depth"] == 10


def test_truy_van_landmark_van_cuon_nong():
    client = _ClientGia([DINH])
    scrape_batch(client, [{"id": "k1", "query": "Dinh III Bảo Đại, Đà Lạt", "destination": "dalat"}])
    assert client.kw["depth"] == 1


def test_category_seeds():
    assert len(available_provinces()) == 34
    # Danh sach tinh khong lan vao danh sach diem den landmark
    assert "areas" not in available_destinations()
    s = load_category_seeds(["lam-dong"])
    assert {x["area"] for x in s} == {"Đà Lạt", "Phan Thiết", "Gia Nghĩa"}
    assert all(x["kind"] == "category" and x["destination"] == "lam-dong" for x in s)
    assert "các khách sạn tốt nhất ở Đà Lạt" in {x["query"] for x in s}
    assert len({x["id"] for x in load_category_seeds()}) == len(load_category_seeds())
    with pytest.raises(ValueError):
        load_category_seeds(["khong-co"])


def test_wait_job_bo_qua_loi_tam_thoi(monkeypatch):
    import requests
    from gmaps.scraper import GmapsScraperClient

    client = GmapsScraperClient("http://x")
    tra_loi = iter([requests.ReadTimeout("cham"), "working", "ok"])

    def job_status(job_id):
        r = next(tra_loi)
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(client, "job_status", job_status)
    assert client.wait_job("j", timeout_seconds=5, poll_seconds=0) == "ok"


def test_don_job_con_sot_truoc_khi_tao_job(monkeypatch):
    from gmaps.scraper import GmapsScraperClient

    client = GmapsScraperClient("http://x")
    lan = iter([
        [{"ID": "p1", "Name": "airflow-a-1", "Status": "pending"},
         {"ID": "w1", "Name": "airflow-a-2", "Status": "working"},
         {"ID": "u1", "Name": "tay-nguoi-dung", "Status": "pending"}],
        [{"ID": "w1", "Name": "airflow-a-2", "Status": "ok"}],
    ])
    da_xoa = []
    monkeypatch.setattr(client, "list_jobs", lambda: next(lan))
    monkeypatch.setattr(client, "delete_job", da_xoa.append)
    client.clear_orphan_jobs(poll_seconds=0)
    # Chi xoa job pending cua airflow, khong dung job nguoi dung tao tay tren web UI
    assert da_xoa == ["p1"]


def test_scraper_treo_thi_bao_loi(monkeypatch):
    from gmaps.scraper import GmapsScraperClient

    client = GmapsScraperClient("http://x")
    monkeypatch.setattr(client, "list_jobs", lambda: [{"ID": "w1", "Name": "airflow-a", "Status": "working"}])
    da_xoa = []
    monkeypatch.setattr(client, "delete_job", da_xoa.append)
    with pytest.raises(ScraperError, match="treo"):
        client.clear_orphan_jobs(busy_timeout=0, poll_seconds=0)
    # Job "ma" sau khi scraper khoi dong lai phai bi xoa de khong chan lan thu sau
    assert da_xoa == ["w1"]


def test_job_mat_thi_bao_loi_ngay(monkeypatch):
    import requests
    from gmaps.scraper import GmapsScraperClient

    client = GmapsScraperClient("http://x")
    resp = requests.Response()
    resp.status_code = 404

    def job_status(job_id):
        raise requests.HTTPError("404", response=resp)

    monkeypatch.setattr(client, "job_status", job_status)
    with pytest.raises(ScraperError, match="khong con"):
        client.wait_job("j", timeout_seconds=60, poll_seconds=0)
