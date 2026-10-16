"""
Danh sach truy van seed cho DAG gmaps_places, doc tu plugins/gmaps/seeds/<diem_den>.txt.

Moi dong mot truy van "ten, khu vuc", co the them " @ goi y dia chi" de chon dung chi nhanh;
dong trong va dong bat dau bang # bi bo qua.
Them diem den moi: tao them file .txt cung thu muc, ten file la khoa diem den.

Ngoai truy van theo ten dia diem (kind="landmark"), con co truy van theo loai hinh
(kind="category"): ghep moi khu vuc trong seeds/category/areas.txt voi moi loai hinh
trong seeds/category/categories.txt. Mot truy van loai hinh ra ca chuc dia diem.
"""

import hashlib
from pathlib import Path

SEED_DIR = Path(__file__).parent / "seeds"
CATEGORY_DIR = SEED_DIR / "category"


def available_destinations() -> list[str]:
    return sorted(p.stem for p in SEED_DIR.glob("*.txt"))


def load_seeds(destinations: list[str]) -> list[dict]:
    """[{"id", "query", "address_hint", "destination"}]; id on dinh theo noi dung truy van."""
    out = []
    for dest in destinations:
        f = SEED_DIR / f"{dest}.txt"
        if not f.exists():
            raise ValueError(f"Khong co file seed cho diem den '{dest}', hien co: {available_destinations()}")
        for line in f.read_text(encoding="utf-8").splitlines():
            q = line.strip()
            if not q or q.startswith("#"):
                continue
            # "ten, khu vuc @ goi y dia chi": goi y dung de chon dung chi nhanh khi nhieu noi
            # trung ten (VD hang chuc quan "Pho Thin 13 Lo Duc" nhung quan goc o 13 P. Lo Duc).
            query, _, hint = (x.strip() for x in q.partition("@"))
            out.append({
                "id": hashlib.sha1(query.encode()).hexdigest()[:12],
                "query": query,
                "address_hint": hint or None,
                "destination": dest,
                "kind": "landmark",
            })
    return out


def _lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def available_provinces() -> list[str]:
    return sorted({line.partition("|")[0].strip() for line in _lines(CATEGORY_DIR / "areas.txt")})


def load_category_seeds(provinces: list[str] | None = None) -> list[dict]:
    """
    Truy van "cac <loai hinh> tot nhat o <khu vuc>" cho cac tinh can cao.

    Args:
        provinces: khoa tinh (cot dau areas.txt), None hoac rong = tat ca

    Returns:
        [{"id", "query", "address_hint", "destination", "kind", "area", "category"}],
        destination la khoa tinh
    """
    areas = []
    for line in _lines(CATEGORY_DIR / "areas.txt"):
        province, _, area = (x.strip() for x in line.partition("|"))
        if not area:
            raise ValueError(f"Dong areas.txt sai dinh dang '<tinh> | <khu vuc>': {line}")
        areas.append((province, area))

    if provinces:
        unknown = set(provinces) - {p for p, _ in areas}
        if unknown:
            raise ValueError(f"Khong co tinh {sorted(unknown)} trong areas.txt, hien co: {available_provinces()}")
        areas = [(p, a) for p, a in areas if p in provinces]

    out = []
    for province, area in areas:
        for category in _lines(CATEGORY_DIR / "categories.txt"):
            query = f"các {category} tốt nhất ở {area}"
            out.append({
                "id": hashlib.sha1(query.encode()).hexdigest()[:12],
                "query": query,
                "address_hint": None,
                "destination": province,
                "kind": "category",
                "area": area,
                "category": category,
            })
    return out
