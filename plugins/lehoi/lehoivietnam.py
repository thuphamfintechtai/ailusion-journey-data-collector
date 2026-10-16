"""
Script cao lich le hoi, su kien tu lehoivietnam.com.vn (ban tieng Viet).

Khong co API cong khai, nhung site co sitemap liet ke day du trang su kien
kem lastmod, va moi trang su kien nhung san JSON-LD schema.org/Event. Danh sach URL lay tu tab "Moi nhat"
(sap theo thoi gian cap nhat) va sitemap, roi doc JSON-LD cho cac truong co
cau truc. JSON-LD cat ngan description o ~160 ky tu
nen mo ta day du, diem nhan, chu de va ma dia diem phai boc them tu HTML.

robots.txt chi chan /out/, /dangnhap, /user/login — cac duong dan dung o day
deu duoc phep.
"""

import gzip
import json
import logging
import re
import time
from datetime import datetime, timezone
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "https://lehoivietnam.com.vn"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap.xml"
LATEST_LIST_URL = f"{BASE_URL}/vi/kham-pha"
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Sitemap tieng Viet ten dang sitemap-events-0001.xml.gz; ban dich co them
# hau to ngon ngu (sitemap-events-en-0001...) nen phai khop dung 4 chu so.
VI_EVENT_SITEMAP_RE = re.compile(r"/sitemap-events-\d{4}\.xml(\.gz)?$")

# URL su kien co hai kieu, ca hai deu bat dau bang id so:
#   /vi/su-kien/3746-e3746-festival-hoa-da-lat-...
#   /vi/su-kien/133-le-hoi-choi-trau-do-son-260203052150-24354
EVENT_ID_RE = re.compile(r"/su-kien/(\d+)-")

USER_AGENT = "ailusion-journey-data-collector/1.0 (+https://github.com/thuphamfintechtai)"
REQUEST_TIMEOUT = 30


# Ty le trang loi toi da trong mot batch. Vuot qua thi coi nhu su co mang/site
# chu khong phai vai trang hong le, cho task fail de Airflow chay lai batch.
MAX_BATCH_ERROR_RATE = 0.2


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "vi"})
    # Thu lai loi ket noi va 5xx voi backoff 2, 4, 8, 16s (~30s) de qua duoc
    # nhung lan rot mang ngan. 404/410 khong retry vi la su kien da go.
    retry = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _get_xml(session: requests.Session, url: str) -> ElementTree.Element:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    content = response.content
    # Server tra file .gz nguyen dang (khong dat Content-Encoding) nen
    # requests khong tu giai nen.
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    return ElementTree.fromstring(content)


def list_sitemap_event_urls() -> dict[str, str | None]:
    """
    Lay toan bo trang su kien tieng Viet tu sitemap.

    Sitemap khong duoc cap nhat deu (luc kiem tra 06/10/2026 lastmod moi nhat
    van la 15/09 va thieu cac id moi), nen chi dung lam nguon bo sung cho
    full refresh, khong dua vao lastmod de loc delta.

    Returns:
        dict url -> lastmod (ISO 8601, co the None)
    """
    session = _session()
    index = _get_xml(session, SITEMAP_INDEX_URL)
    sitemap_urls = [
        loc.text.strip()
        for loc in index.findall("sm:sitemap/sm:loc", SITEMAP_NS)
        if loc.text and VI_EVENT_SITEMAP_RE.search(loc.text.strip())
    ]
    if not sitemap_urls:
        raise RuntimeError(f"Khong tim thay sitemap su kien trong {SITEMAP_INDEX_URL}")

    events = {}
    for sitemap_url in sitemap_urls:
        logger.info(f"Dang doc sitemap {sitemap_url}")
        urlset = _get_xml(session, sitemap_url)
        for node in urlset.findall("sm:url", SITEMAP_NS):
            loc = node.findtext("sm:loc", default="", namespaces=SITEMAP_NS).strip()
            if "/vi/su-kien/" in loc:
                lastmod = node.findtext("sm:lastmod", default="", namespaces=SITEMAP_NS).strip()
                events[loc] = lastmod or None

    logger.info(f"Sitemap co {len(events)} su kien")
    return events


def list_latest_event_urls(max_pages: int | None, delay_seconds: float = 1.0) -> list[str]:
    """
    Doc tab "Moi nhat" o /vi/kham-pha, sap theo thoi gian cap nhat giam dan.

    Args:
        max_pages: so trang toi da can doc, None la doc den het

    Returns:
        list url theo dung thu tu tren site (moi cap nhat dung truoc)
    """
    session = _session()
    urls: list[str] = []
    seen: set[str] = set()
    page = 1

    while max_pages is None or page <= max_pages:
        if page > 1:
            time.sleep(delay_seconds)
        response = session.get(
            LATEST_LIST_URL, params={"tab": "latest", "page": page}, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        # Chi lay trong article cua danh sach — sidebar "Cap nhat moi" co link
        # su kien lap lai o moi trang, lay ca vao se khong bao gio dung duoc.
        page_urls = [
            BASE_URL + a["href"]
            for a in soup.select("article[data-event-item] h3 a[href]")
            if "/su-kien/" in a["href"]
        ]
        new_urls = [u for u in page_urls if u not in seen]
        # Qua trang cuoi site van tra ve trang cuoi chu khong 404, nen dung
        # khi trang khong con url moi.
        if not new_urls:
            break
        urls.extend(new_urls)
        seen.update(new_urls)
        page += 1

    logger.info(f"Tab Moi nhat: doc {page - 1} trang, {len(urls)} su kien")
    return urls


def discover_event_urls(full_refresh: bool, max_pages: int) -> list[dict]:
    """
    Gom danh sach trang su kien can cao.

    Delta: chi doc max_pages trang dau cua tab "Moi nhat".
    Full: doc het tab "Moi nhat" va hop voi sitemap, vi moi nguon deu co the
    thieu (sitemap cu, listing co the an su kien da qua han).

    Returns:
        list dict {url, lastmod}, sap xep theo url de cac batch on dinh
    """
    if full_refresh:
        events = list_sitemap_event_urls()
        latest = list_latest_event_urls(max_pages=None)
    else:
        events = {}
        latest = list_latest_event_urls(max_pages=max_pages)

    for url in latest:
        events.setdefault(url, None)

    logger.info(f"Tong cong {len(events)} su kien can cao ({'full' if full_refresh else 'delta'})")
    return [{"url": url, "lastmod": events[url]} for url in sorted(events)]


def _find_event_jsonld(soup: BeautifulSoup) -> dict | None:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except json.JSONDecodeError:
            continue
        # Mot so trang co the boc trong @graph hoac list
        candidates = data if isinstance(data, list) else data.get("@graph", [data])
        for item in candidates:
            if isinstance(item, dict) and item.get("@type") == "Event":
                return item
    return None


def _text(node) -> str | None:
    if node is None:
        return None
    text = " ".join(node.get_text(" ", strip=True).split())
    return text or None


def _schema_value(value: str | None) -> str | None:
    """https://schema.org/EventScheduled -> EventScheduled."""
    if not value:
        return None
    return value.rsplit("/", 1)[-1]


def parse_event_page(html: str, url: str) -> dict:
    """
    Boc mot trang su kien thanh dict phang.

    Raises:
        ValueError: trang khong co JSON-LD Event (site doi giao dien,
            hoac URL khong con la trang su kien)
    """
    soup = BeautifulSoup(html, "html.parser")
    ld = _find_event_jsonld(soup)
    if ld is None:
        raise ValueError(f"Khong thay JSON-LD Event tai {url}")

    location = ld.get("location") or {}
    address = location.get("address") or {}
    organizer = ld.get("organizer") or {}
    offers = ld.get("offers") or {}
    images = ld.get("image") or []
    if isinstance(images, str):
        images = [images]
    # Trang khong co anh rieng thi JSON-LD dien logo site, bo di cho do nhieu
    images = [img for img in images if "/images/logo/" not in img]

    view = soup.select_one(".v2-event-view") or soup

    # Mo ta day du la doan <p> dau tien ngay sau tieu de h1
    description = None
    title = view.select_one("h1.v2-title")
    if title:
        para = title.find_next_sibling("p", class_="mb-0")
        description = _text(para)

    highlights = []
    tags = []
    for card in view.select("section.v2-card"):
        heading = _text(card.select_one(".v2-card-title"))
        if heading == "Điểm nhấn":
            highlights = [t for li in card.select("li") if (t := _text(li))]
        elif heading == "Chủ đề liên quan":
            tags = [
                {"name": _text(a), "url": BASE_URL + a["href"]}
                for a in card.select("a.v2-tag")
                if a.get("href")
            ]

    # data-loc2 la cap tinh/thanh, data-loc3 la cap xa/phuong (0 neu khong co).
    # Lay tu link dia diem chinh ngay duoi ngay to chuc.
    loc_link = view.select_one(".v2-detail-priority-loc a.lh-loc-link")
    loc2 = loc_link.get("data-loc2") if loc_link else None
    loc3 = loc_link.get("data-loc3") if loc_link else None

    admin_areas = [
        {"name": _text(a), "url": BASE_URL + a["href"]}
        for a in view.select(".lh-admin-area a.lh-admin-area__link")
        if a.get("href")
    ]

    id_match = EVENT_ID_RE.search(url)

    return {
        "event_id": int(id_match.group(1)) if id_match else None,
        "url": ld.get("url") or url,
        "name": ld.get("name"),
        "start_date": ld.get("startDate"),
        "end_date": ld.get("endDate"),
        "date_display": _text(view.select_one(".v2-detail-priority-date")),
        "event_status": _schema_value(ld.get("eventStatus")),
        "attendance_mode": _schema_value(ld.get("eventAttendanceMode")),
        "location_name": location.get("name"),
        "street_address": address.get("streetAddress"),
        "address_region": address.get("addressRegion"),
        "address_country": address.get("addressCountry"),
        "loc2_id": loc2 if loc2 and loc2 != "0" else None,
        "loc3_id": loc3 if loc3 and loc3 != "0" else None,
        "admin_areas": admin_areas,
        "organizer": organizer.get("name"),
        "price": offers.get("price"),
        "price_currency": offers.get("priceCurrency"),
        "description": description or ld.get("description"),
        "highlights": highlights,
        "tags": tags,
        "images": images,
    }


def fetch_events(entries: list[dict], delay_seconds: float = 1.0) -> list[dict]:
    """
    Cao lan luot mot batch trang su kien.

    Chay tuan tu co nghi giua cac request de khong don tai len site; song song
    hoa nam o tang DAG (nhieu batch) va bi gioi han o do.

    Loi o tung trang chi ghi log va bo qua, de mot trang hong khong lam
    hong ca batch. 404/410 la su kien da go, khong tinh la loi.
    """
    session = _session()
    results = []
    failed = 0

    for i, entry in enumerate(entries):
        if i:
            time.sleep(delay_seconds)
        url = entry["url"]
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code in (404, 410):
                logger.info(f"Su kien da bi go: {url}")
                continue
            response.raise_for_status()
            event = parse_event_page(response.text, url)
        except Exception as e:
            failed += 1
            logger.error(f"Loi khi cao {url}: {e}")
            continue

        event["lastmod"] = entry.get("lastmod")
        event["fetched_at"] = datetime.now(timezone.utc).isoformat()
        results.append(event)

    logger.info(f"Batch xong: {len(results)} thanh cong, {failed} loi / {len(entries)} trang")
    # Loi nhieu thuong la do rot mang, bi chan hoac site doi giao dien — de task
    # fail cho Airflow retry va hien do tren UI, thay vi am tham bo qua trang.
    if entries and failed / len(entries) > MAX_BATCH_ERROR_RATE:
        raise RuntimeError(f"Batch loi {failed}/{len(entries)} trang, vuot nguong {MAX_BATCH_ERROR_RATE:.0%}")
    return results

