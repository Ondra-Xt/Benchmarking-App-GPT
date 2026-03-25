from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple
import json
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

BASE = "https://catalog.geberit.de"
SCOPE = "/de-DE"
PRIMARY_SEED = f"{BASE}{SCOPE}/product/PRO_3932352"
MARKETING_SEED = "https://www.geberit.de/badezimmerprodukte/duschen-badewannenablaeufe/duschen/duschrinnen-geberit-cleanline/"
PUBLIC_SEEDS = [
    MARKETING_SEED,
    "https://www.geberit.de/landingpages/geberit-cleanline30/",
]
CATALOG_SYSTEM_SEEDS = [
    "https://catalog.geberit.de/de-DE/systems/CH3_3294141/products/",
    "https://catalog.geberit.de/de-DE/product/PRO_170941/",
]

ACCESSORY_RE = re.compile(
    r"verbindungsst[üu]ck|verl[äa]ngerung|reinigungszubeh[öo]r|allgemeines\s+zubeh[öo]r|\bzubeh[öo]r\b|ablaufkette",
    re.IGNORECASE,
)
CLEANLINE_RE = re.compile(r"cleanline\s*(20|50|60|80)?", re.IGNORECASE)
DRAIN_RE = re.compile(r"duschrinne|duschprofil|duschablauf|duschentw[äa]sserung|shower\s*channel|shower\s*drain", re.IGNORECASE)
ROHBAU_RE = re.compile(r"rohbauset|rohbau\s*set|rohbau", re.IGNORECASE)
WRONG_FAMILY_RE = re.compile(
    r"ausgussbecken|rohrbogengeruchsverschluss|waschtisch|m[öo]belwaschtisch|sp[üu]lkasten|\bwc\b|lavabo|basin|sink|clean\s*drain",
    re.IGNORECASE,
)
HARD_WRONG_FAMILY_RE = re.compile(
    r"waschtisch|m[öo]belwaschtisch|sp[üu]lkasten|\bwc\b|ausgussbecken|geruchsverschluss|siphon",
    re.IGNORECASE,
)

ARTICLE_RE = re.compile(r"\b(\d{3}\.\d{3}[A-Z0-9\.]*|\d{6,}[A-Z0-9\.]*)\b", re.IGNORECASE)
FLOW_LPS_RE = re.compile(r"(\d+(?:[\.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
DN_PAIR_RE = re.compile(r"\bDN\s*(\d{2,3})\s*/\s*(?:DN\s*)?(\d{2,3})\b", re.IGNORECASE)
DN_SINGLE_RE = re.compile(r"\b(?:nennweite\s*)?DN\s*(\d{2,3})\b", re.IGNORECASE)
HEIGHT_RANGE_RE = re.compile(r"(?:einbauh(?:ö|oe)he|estrichh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm", re.IGNORECASE)
HEIGHT_SINGLE_RE = re.compile(r"(?:einbauh(?:ö|oe)he|estrichh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*mm", re.IGNORECASE)
TRAP_SEAL_RE = re.compile(r"sperrwasserh(?:ö|oe)he|geruchsverschluss|verschlussh(?:ö|oe)he", re.IGNORECASE)
LEN_MM_RE = re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE)
LEN_RANGE_CM_RE = re.compile(r"\b(\d{2,3})\s*[-–]\s*(\d{2,3})\s*cm\b", re.IGNORECASE)
LEN_RANGE_MM_RE = re.compile(r"\b(\d{3,4})\s*[-–]\s*(\d{3,4})\s*mm\b", re.IGNORECASE)
MATERIAL_TOKEN_RE = re.compile(r"\b(1\.4404|1\.4571|1\.4301|316L|316|304|V4A|V2A)\b", re.IGNORECASE)


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _main_flat_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    main = soup.select_one("main")
    if main is not None:
        return _clean_text(main.get_text(" ", strip=True) or "")
    for sel in ["header", "nav", "footer", "aside"]:
        for tag in soup.select(sel):
            tag.decompose()
    return _clean_text(soup.get_text(" ", strip=True) or "")


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _canonicalize_url(url: str) -> str:
    try:
        p = urlparse((url or "").strip())
    except Exception:
        return (url or "").split("#", 1)[0].split("?", 1)[0]
    path = (p.path or "/").replace("//", "/")
    if path != "/":
        path = path.rstrip("/") + "/"
    return f"{p.scheme}://{p.netloc}{path}"


def _in_scope(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.netloc.endswith("catalog.geberit.de") and (p.path or "").startswith(SCOPE)
    except Exception:
        return False


def _is_public_geberit_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.netloc.endswith("geberit.de")
    except Exception:
        return False


def _is_landing_page(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    path = (p.path or "").lower()
    return "/landingpages/" in path or path.rstrip("/") == "/badezimmerprodukte/duschen-badewannenablaeufe/duschen/duschrinnen-geberit-cleanline"




def _is_catalog_detail_page(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    path = (p.path or "")
    return p.netloc.endswith("catalog.geberit.de") and bool(re.search(r"/de-DE/product/[^/]+/?$", path, re.IGNORECASE))


def _is_catalog_pro_page(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    path = (p.path or "")
    return p.netloc.endswith("catalog.geberit.de") and bool(re.search(r"/de-DE/product/PRO_[^/]+/?$", path, re.IGNORECASE))


def _is_system_listing_page(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    path = (p.path or "")
    return p.netloc.endswith("catalog.geberit.de") and "/systems/" in path and path.rstrip("/").endswith("/products")

def _extract_title(html: str, fallback_url: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    h1 = soup.select_one("h1")
    if h1:
        t = _clean_text(h1.get_text(" ", strip=True))
        if t:
            return t
    t = soup.select_one("title")
    if t:
        x = _clean_text(t.get_text(" ", strip=True))
        if x:
            return x
    return fallback_url.rstrip("/").split("/")[-1].replace("-", " ")


def _article_from_text(text: str) -> Optional[str]:
    m = ARTICLE_RE.search(text or "")
    return m.group(1) if m else None


def _normalize_article(article: str) -> Optional[str]:
    a = re.sub(r"[^0-9A-Za-z]", "", (article or "")).upper()
    return a or None


def _product_id(url: str, text: str) -> str:
    article = _normalize_article(_article_from_text(text) or "")
    if article:
        return f"geberit-{article}"
    return f"geberit-{abs(hash(url))}"


def _length_info(text: str, target_mm: int) -> Tuple[Optional[int], str, Optional[str], bool]:
    for m in LEN_RANGE_CM_RE.finditer(text or ""):
        a, b = int(m.group(1)) * 10, int(m.group(2)) * 10
        lo, hi = (a, b) if a <= b else (b, a)
        return None, "variable", f"{lo}-{hi} mm", lo <= target_mm <= hi
    for m in LEN_RANGE_MM_RE.finditer(text or ""):
        a, b = int(m.group(1)), int(m.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        return None, "variable", f"{lo}-{hi} mm", lo <= target_mm <= hi
    vals = [int(m.group(1)) for m in LEN_MM_RE.finditer(text or "") if 300 <= int(m.group(1)) <= 2500]
    if vals:
        return max(vals), "fixed", None, False
    return None, "unknown", None, False


def _extract_flow(flat: str) -> Tuple[List[float], Optional[Tuple[int, int]]]:
    vals: List[float] = []
    first: Optional[Tuple[int, int]] = None
    src = flat or ""
    for km in re.finditer(r"ablaufleistung|abflussleistung", src, re.IGNORECASE):
        lo = max(0, km.start() - 20)
        hi = min(len(src), km.end() + 85)
        part = src[lo:hi]
        for m in FLOW_LPS_RE.finditer(part):
            prev1 = part.lower().rfind("ablaufleistung", 0, m.start() + 1)
            prev2 = part.lower().rfind("abflussleistung", 0, m.start() + 1)
            prev = max(prev1, prev2)
            if prev < 0 or (m.start() - prev) > 36:
                continue
            if "l/s" in part[prev:m.start()].lower():
                continue
            try:
                v = float(m.group(1).replace(",", "."))
            except Exception:
                continue
            if 0.10 <= v <= 3.0:
                vals.append(v)
                if first is None:
                    first = (lo + m.start(), lo + m.end())
    return sorted(set(vals)), first


def _extract_dn(flat: str) -> Tuple[List[str], Optional[Tuple[int, int]]]:
    dns: List[str] = []
    first: Optional[Tuple[int, int]] = None
    for m in DN_PAIR_RE.finditer(flat or ""):
        for dn in (f"DN{m.group(1)}", f"DN{m.group(2)}"):
            if dn not in dns:
                dns.append(dn)
        if first is None:
            first = (m.start(), m.end())
    for m in DN_SINGLE_RE.finditer(flat or ""):
        dn = f"DN{m.group(1)}"
        if dn not in dns:
            dns.append(dn)
            if first is None:
                first = (m.start(), m.end())
    return sorted(dns), first


def _extract_height(flat: str) -> Tuple[Optional[int], Optional[int], Optional[Tuple[int, int]]]:
    h = HEIGHT_RANGE_RE.search(flat or "")
    if h and not TRAP_SEAL_RE.search(_snippet(flat, h.start(), h.end(), pad=20)):
        a, b = int(h.group(1)), int(h.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if 20 <= lo <= 350 and 20 <= hi <= 350:
            return lo, hi, (h.start(), h.end())
    hs = HEIGHT_SINGLE_RE.search(flat or "")
    if hs and not TRAP_SEAL_RE.search(_snippet(flat, hs.start(), hs.end(), pad=20)):
        v = int(hs.group(1))
        if 20 <= v <= 350:
            return v, v, (hs.start(), hs.end())
    return None, None, None


def _extract_material(flat: str) -> Tuple[Optional[str], Optional[str], Optional[Tuple[int, int]]]:
    src = flat or ""
    token_match = MATERIAL_TOKEN_RE.search(src)
    if token_match:
        token = token_match.group(1).upper()
        v4a = "yes" if token in {"1.4404", "1.4571", "316", "316L", "V4A"} else "no"
        return token, v4a, token_match.span()

    text_match = re.search(r"\b(edelstahl|stahl|kunststoff|metall)\b", src, re.IGNORECASE)
    if text_match:
        token = text_match.group(1).lower()
        v4a = "yes" if token == "edelstahl" and bool(re.search(r"\bv4a\b|1\.4404|1\.4571|316", src, re.IGNORECASE)) else None
        return token, v4a, text_match.span()

    return None, None, None


def _extract_din_compliance(flat: str) -> Tuple[Optional[str], Optional[Tuple[int, int]], Optional[str], Optional[Tuple[int, int]]]:
    src = flat or ""
    en_match = re.search(r"(DIN\s*)?EN\s*1253", src, re.IGNORECASE)
    din18534_match = re.search(
        r"(DIN\s*18534|Verbundabdichtung\s+nach\s+DIN\s*18534|Dichtvlies[^\.\n]{0,80}DIN\s*18534|abdichtung[^\.\n]{0,80}DIN\s*18534)",
        src,
        re.IGNORECASE,
    )
    return (
        "yes" if en_match else None,
        en_match.span() if en_match else None,
        "yes" if din18534_match else None,
        din18534_match.span() if din18534_match else None,
    )


def _extract_sealing_fleece(flat: str) -> Tuple[Optional[str], Optional[Tuple[int, int]]]:
    src = flat or ""
    match = re.search(r"(dichtvlies|abdichtungs?vlies|sealing\s+fleece)[^\.\n]{0,80}(vormontiert|vorinstalliert|werkseitig\s+montiert|preassembled)", src, re.IGNORECASE)
    if match:
        return "yes", match.span()
    return None, None


def _extract_colours_count(flat: str) -> Tuple[Optional[int], Optional[Tuple[int, int]]]:
    src = flat or ""
    count_match = re.search(r"\b(\d+)\s*(?:farben|colors?|colour variants?)\b", src, re.IGNORECASE)
    if count_match:
        return int(count_match.group(1)), count_match.span()

    list_match = re.search(r"(?:farben|colors?)\s*:\s*([A-Za-zÄÖÜäöüß ,/]+)", src, re.IGNORECASE)
    if list_match:
        items = [x.strip() for x in re.split(r"[,/]", list_match.group(1)) if x.strip()]
        if items:
            return len(items), list_match.span()
    return None, None


def _extract_catalog_links(html: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html or "", "lxml")
    out: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        u = _canonicalize_url(urljoin(base_url, href))
        if _in_scope(u) and "/product/" in u and CLEANLINE_RE.search(f"{txt} {href} {u}"):
            out.add(u)
    return out


def _has_article_table_signals(html: str) -> bool:
    soup = BeautifulSoup(html or "", "lxml")
    table_text = _clean_text(" ".join(t.get_text(" ", strip=True) for t in soup.select("table")))
    if not table_text:
        return False
    has_art = bool(re.search(r"art\.?\s*-?\s*nr|artikel\s*-?\s*nr", table_text, re.IGNORECASE))
    has_perf = bool(re.search(r"ablaufleistung|l\s*/\s*s", table_text, re.IGNORECASE))
    has_dn = bool(re.search(r"\bdn\s*\d{2,3}\b|\bdn\b", table_text, re.IGNORECASE))
    has_dims = bool(re.search(r"\bL\s*cm\b", table_text, re.IGNORECASE) and re.search(r"\bH\s*cm\b", table_text, re.IGNORECASE))
    row_count = len(soup.select("table tr"))
    return has_art and has_dn and (has_perf or has_dims or row_count >= 2)


def _select_article_variant_from_table(html: str, target_mm: int = 1200, tolerance_mm: int = 100) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "lxml")
    rows: List[Dict[str, Any]] = []
    for tr in soup.select("table tr"):
        cells = tr.select("th,td")
        if not cells:
            continue
        row_text = _clean_text(" ".join(c.get_text(" ", strip=True) for c in cells))
        article = _article_from_text(row_text)
        if not article:
            continue
        length_mm, _, _, _ = _length_info(row_text, target_mm)
        flow_opts = []
        for fm in FLOW_LPS_RE.finditer(row_text):
            try:
                fv = float(fm.group(1).replace(",", "."))
            except Exception:
                continue
            if 0.10 <= fv <= 3.0:
                flow_opts.append(fv)
        flow_opts = sorted(set(flow_opts))
        dns, _ = _extract_dn(row_text)
        hmin, hmax, _ = _extract_height(row_text)
        rows.append({
            "article_no": article,
            "row_text": row_text,
            "length_mm": length_mm,
            "flow_opts": flow_opts,
            "dns": dns,
            "hmin": hmin,
            "hmax": hmax,
        })
    if not rows:
        return None

    in_range = [r for r in rows if isinstance(r.get("length_mm"), int) and abs(int(r["length_mm"]) - target_mm) <= tolerance_mm]
    if in_range:
        return sorted(in_range, key=lambda r: abs(int(r["length_mm"]) - target_mm))[0]

    with_len = [r for r in rows if isinstance(r.get("length_mm"), int)]
    if with_len:
        return sorted(with_len, key=lambda r: abs(int(r["length_mm"]) - target_mm))[0]
    return rows[0]


def _extract_public_links(html: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html or "", "lxml")
    out: Set[str] = set()
    html_norm = (html or "").replace("\\/", "/")
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        u = _canonicalize_url(urljoin(base_url, href))
        if _is_public_geberit_url(u) and CLEANLINE_RE.search(f"{txt} {href} {u}"):
            out.add(u)
        if _in_scope(u) and "/product/" in u and CLEANLINE_RE.search(f"{txt} {href} {u}"):
            out.add(u)
    for m in re.finditer(r"https://catalog\.geberit\.de/de-DE/product/[A-Za-z0-9\._-]+", html_norm, re.IGNORECASE):
        out.add(_canonicalize_url(m.group(0)))
    for m in re.finditer(r"(?:https://catalog\.geberit\.de)?/de-DE/product/[A-Za-z0-9\._-]+", html_norm, re.IGNORECASE):
        out.add(_canonicalize_url(urljoin(BASE, m.group(0))))
    for m in re.finditer(r"https://www\.geberit\.de/PRO_[A-Za-z0-9_-]+-DE_DE/?", html_norm, re.IGNORECASE):
        out.add(_canonicalize_url(m.group(0)))
    for m in re.finditer(r"/PRO_[A-Za-z0-9_-]+-DE_DE/?", html_norm, re.IGNORECASE):
        out.add(_canonicalize_url(urljoin(base_url, m.group(0))))
    return out




def _wrong_product_family(url: str, title: str, flat: str, html: str = "") -> bool:
    txt = f"{url} {title} {flat}".lower()
    if HARD_WRONG_FAMILY_RE.search(txt):
        return True
    if not WRONG_FAMILY_RE.search(txt):
        return False
    if CLEANLINE_RE.search(txt) or DRAIN_RE.search(txt) or _has_article_table_signals(html):
        return False
    return True

def _is_cleanline_product_page(url: str, title: str, flat: str, from_cleanline_context: bool = False, html: str = "") -> bool:
    txt = f"{url} {title} {flat}".lower()
    is_catalog_detail = _is_catalog_detail_page(url)
    is_catalog_pro = _is_catalog_pro_page(url)
    if ACCESSORY_RE.search(txt):
        return False
    if _is_public_geberit_url(url) and _is_landing_page(url):
        return False
    if not (_is_catalog_detail_page(url) or _is_public_geberit_url(url)):
        return False
    if from_cleanline_context and is_catalog_detail and is_catalog_pro:
        if CLEANLINE_RE.search(txt) or DRAIN_RE.search(txt) or _has_article_table_signals(html):
            return True
        return False
    if not from_cleanline_context and not CLEANLINE_RE.search(txt):
        return False
    if not (CLEANLINE_RE.search(txt) or DRAIN_RE.search(txt)):
        return False
    return True


def _extract_rohbau_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[str] = []
    seen = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        txt = _clean_text(a.get_text(" ", strip=True))
        u = _canonicalize_url(urljoin(base_url, href))
        if not _in_scope(u):
            continue
        if ROHBAU_RE.search(f"{txt} {u}") and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    tol = int(tolerance_mm)
    min_len, max_len = max(0, want - tol), want + tol

    out: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    queue: List[Tuple[str, bool]] = [(_canonicalize_url(u), True) for u in CATALOG_SYSTEM_SEEDS]
    seen: Set[str] = set()
    pages: Dict[str, bool] = {}

    while queue and len(seen) < 120:
        u, from_cleanline_context = queue.pop(0)
        u = _canonicalize_url(u)
        if u in seen:
            continue
        seen.add(u)

        st, final, html, err = _safe_get_text(u)
        if st != 200 or not html:
            debug.append({"site": "geberit", "seed_url": u, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "crawl", "is_index": None})
            continue

        final_c = _canonicalize_url(final)
        page_ctx = from_cleanline_context or bool(CLEANLINE_RE.search(f"{final_c} {html}"))
        if _is_public_geberit_url(final_c) or _is_system_listing_page(final_c) or (_is_catalog_detail_page(final_c) and page_ctx):
            pages[final_c] = pages.get(final_c, False) or page_ctx

        for cand in _extract_public_links(html, final_c):
            if cand not in seen and not any(cu == cand for cu, _ in queue):
                queue.append((cand, pages.get(final_c, page_ctx)))

    product_urls: List[str] = []
    bom_urls: List[str] = []
    unknown_length_count = 0
    dropped: List[Dict[str, str]] = []
    dropped_reason: Dict[str, int] = {}
    rejected_lengths: List[Dict[str, Any]] = []
    missing_length_rows: List[Dict[str, str]] = []
    landing_urls = [u for u in sorted(pages) if _is_landing_page(u)]
    listing_urls = [u for u in sorted(pages) if _is_system_listing_page(u)]
    detail_urls = [u for u in sorted(pages) if _is_catalog_detail_page(u) and not _is_system_listing_page(u)]

    def add_drop(url: str, reason: str, extra: Optional[Dict[str, Any]] = None) -> None:
        row: Dict[str, Any] = {"url": url, "reason": reason}
        if extra:
            row.update(extra)
        dropped.append(row)
        dropped_reason[reason] = dropped_reason.get(reason, 0) + 1

    for u in sorted(pages):
        st, final, html, err = _safe_get_text(u)
        if st != 200 or not html:
            continue

        title = _extract_title(html, final)
        flat = _main_flat_text(html)
        if _is_system_listing_page(u):
            add_drop(u, "listing_page_intermediate")
            continue
        if _wrong_product_family(u, title, flat, html=html):
            add_drop(u, "wrong_product_family")
            continue
        if not _is_cleanline_product_page(u, title, flat, from_cleanline_context=pages.get(u, False), html=html):
            add_drop(u, "not_cleanline_product_page")
            continue

        length_mm: Optional[int] = None
        length_mode = "unknown"
        range_txt: Optional[str] = None
        range_match = False
        if _is_catalog_pro_page(u):
            variant = _select_article_variant_from_table(html, target_mm=want, tolerance_mm=tol)
            if variant and isinstance(variant.get("length_mm"), int):
                length_mm = int(variant["length_mm"])
                length_mode = "fixed"
        if length_mode == "unknown":
            length_mm, length_mode, range_txt, range_match = _length_info(f"{title} {flat}", want)

        if length_mode == "fixed" and length_mm is not None and not (min_len <= length_mm <= max_len):
            rejected_lengths.append({"url": u, "length_mm": length_mm, "target_mm": want})
            add_drop(u, "length_out_of_range", {"length_mm": length_mm})
            continue
        if length_mode == "variable" and not range_match:
            rejected_lengths.append({"url": u, "length_range": range_txt or "unknown", "target_mm": want})
            add_drop(u, "variable_length_no_match", {"length_range": range_txt or "unknown"})
            continue
        if length_mode == "unknown":
            missing_length_rows.append({"url": u, "title": title})

        pid = _product_id(u, f"{title} {flat}")
        out.append({
            "manufacturer": "geberit",
            "product_id": pid,
            "product_family": "CleanLine",
            "product_name": f"{title} ({length_mm} mm)" if length_mm is not None else title,
            "product_url": u,
            "sources": u,
            "candidate_type": "drain",
            "complete_system": "yes",
            "selected_length_mm": want,
            "length_mode": length_mode,
            "length_delta_mm": None if length_mm is None else (length_mm - want),
            "discovery_evidence": "Length (range)" if length_mode == "variable" else None,
        })
        product_urls.append(u)
        if length_mode == "unknown":
            unknown_length_count += 1
        bom_urls.extend(_extract_rohbau_links(html, u))

    dedup: Dict[str, Dict[str, Any]] = {}
    for r in out:
        pid = str(r.get("product_id") or "").strip()
        if pid and pid not in dedup:
            dedup[pid] = r

    debug.append({
        "site": "geberit",
        "seed_url": MARKETING_SEED,
        "status_code": 200 if dedup else None,
        "final_url": MARKETING_SEED,
        "error": "" if dedup else "No accepted candidates.",
        "candidates_found": len(dedup),
        "method": "summary",
        "is_index": None,
        "total_found_links": len(pages),
        "landing_pages_found": len(landing_urls),
        "listing_pages_found": len(listing_urls),
        "detail_pages_found": len(detail_urls),
        "products_count": sum(1 for r in dedup.values() if str(r.get("candidate_type")) == "drain"),
        "bom_options_count": len(set(bom_urls)),
        "unknown_length_count": unknown_length_count,
        "dropped_reason": json.dumps(dropped_reason, ensure_ascii=False),
        "dropped_reason_counts": json.dumps(dropped_reason, ensure_ascii=False),
        "accepted_product_links": json.dumps(product_urls[:20], ensure_ascii=False),
        "dropped_links": json.dumps(dropped[:20], ensure_ascii=False),
        "sample_landing_urls": json.dumps(landing_urls[:10], ensure_ascii=False),
        "sample_listing_urls": json.dumps(listing_urls[:10], ensure_ascii=False),
        "sample_detail_urls": json.dumps(detail_urls[:10], ensure_ascii=False),
        "sample_accepted_urls": json.dumps(product_urls[:10], ensure_ascii=False),
        "sample_rejected_urls": json.dumps([row.get("url") for row in dropped[:10]], ensure_ascii=False),
        "sample_product_urls": json.dumps(product_urls[:10], ensure_ascii=False),
        "sample_products_urls": json.dumps(product_urls[:10], ensure_ascii=False),
        "sample_rejected_lengths": json.dumps(rejected_lengths[:10], ensure_ascii=False),
        "sample_missing_length_rows": json.dumps(missing_length_rows[:10], ensure_ascii=False),
        "sample_bom_urls": json.dumps(list(dict.fromkeys(bom_urls))[:10], ensure_ascii=False),
    })

    return list(dedup.values()), debug


def extract_parameters(product_url: str) -> Dict[str, Any]:
    res: Dict[str, Any] = {
        "flow_rate_lps": None,
        "flow_rate_raw_text": None,
        "flow_rate_unit": None,
        "flow_rate_status": None,
        "flow_rate_lps_options": None,
        "material_detail": None,
        "material_v4a": None,
        "din_en_1253_cert": None,
        "din_18534_compliance": None,
        "height_adj_min_mm": None,
        "height_adj_max_mm": None,
        "outlet_dn": None,
        "outlet_dn_default": None,
        "outlet_dn_options_json": None,
        "sealing_fleece_preassembled": None,
        "colours_count": None,
        "resolved_length_mm": None,
        "evidence": [],
    }

    src = _canonicalize_url((product_url or "").strip())
    st, final, html, err = _safe_get_text(src)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))
    if st != 200 or not html:
        return res

    flat = _main_flat_text(html)

    if re.search(r"/product/pro_[a-z0-9_-]+/?$", src, re.IGNORECASE):
        variant = _select_article_variant_from_table(html, target_mm=1200, tolerance_mm=100)
        if variant:
            row_text = str(variant.get("row_text") or "")
            row_len = variant.get("length_mm")
            if isinstance(row_len, int):
                res["resolved_length_mm"] = row_len
                res["evidence"].append(("Artikel table length", f"{row_len} mm", final))
            flow_opts = variant.get("flow_opts") or []
            if flow_opts:
                res["flow_rate_lps_options"] = json.dumps(flow_opts, ensure_ascii=False)
                res["flow_rate_lps"] = max(flow_opts)
                res["flow_rate_unit"] = "l/s"
                res["flow_rate_status"] = "ok"
            dns = variant.get("dns") or []
            if dns:
                res["outlet_dn"] = "/".join(dns)
                res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
                res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
            hmin = variant.get("hmin")
            hmax = variant.get("hmax")
            if isinstance(hmin, int) and isinstance(hmax, int):
                res["height_adj_min_mm"] = hmin
                res["height_adj_max_mm"] = hmax
            art = variant.get("article_no")
            if art:
                res["evidence"].append(("Artikel table article", str(art), final))
            if row_text:
                res["evidence"].append(("Artikel table row", row_text[:220], final))

    material_detail, material_v4a, material_span = _extract_material(flat)
    if material_detail:
        res["material_detail"] = material_detail
        res["material_v4a"] = material_v4a
        if material_span:
            res["evidence"].append(("Material", _snippet(flat, material_span[0], material_span[1]), final))

    din_en_1253_cert, en_span, din_18534_compliance, din18534_span = _extract_din_compliance(flat)
    if din_en_1253_cert:
        res["din_en_1253_cert"] = din_en_1253_cert
        if en_span:
            res["evidence"].append(("DIN EN 1253", _snippet(flat, en_span[0], en_span[1]), final))
    if din_18534_compliance:
        res["din_18534_compliance"] = din_18534_compliance
        if din18534_span:
            res["evidence"].append(("DIN 18534", _snippet(flat, din18534_span[0], din18534_span[1]), final))

    sealing_fleece, fleece_span = _extract_sealing_fleece(flat)
    if sealing_fleece:
        res["sealing_fleece_preassembled"] = sealing_fleece
        if fleece_span:
            res["evidence"].append(("Sealing fleece", _snippet(flat, fleece_span[0], fleece_span[1]), final))

    colours_count, colours_span = _extract_colours_count(flat)
    if colours_count is not None:
        res["colours_count"] = colours_count
        if colours_span:
            res["evidence"].append(("Colours", _snippet(flat, colours_span[0], colours_span[1]), final))

    len_mm, len_mode, range_txt, _ = _length_info(flat, 1200)
    if len_mm is not None:
        res["resolved_length_mm"] = len_mm
        res["evidence"].append(("Length", f"{len_mm} mm", final))
    elif len_mode == "variable" and range_txt:
        res["evidence"].append(("Length (range)", range_txt, final))

    dns, dn_span = _extract_dn(flat)
    if dns:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        if dn_span:
            res["evidence"].append(("Outlet DN", _snippet(flat, dn_span[0], dn_span[1]), final))

    flow_opts, flow_span = _extract_flow(flat)
    if flow_opts:
        res["flow_rate_lps_options"] = json.dumps(flow_opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(flow_opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"
        if flow_span:
            res["evidence"].append(("Flow rate (Ablaufleistung)", _snippet(flat, flow_span[0], flow_span[1]), final))

    hmin, hmax, hspan = _extract_height(flat)
    if hmin is not None and hmax is not None:
        res["height_adj_min_mm"] = hmin
        res["height_adj_max_mm"] = hmax
        if hspan:
            res["evidence"].append(("Installation height (mm)", _snippet(flat, hspan[0], hspan[1]), final))

    return res


def _parse_rohbauset_page(url: str) -> Optional[Dict[str, Any]]:
    st, final, html, _ = _safe_get_text(url)
    if st != 200 or not html:
        return None
    title = _extract_title(html, final)
    flat = _main_flat_text(html)
    article = _article_from_text(f"{title} {flat}")
    aid = _normalize_article(article or "")
    dns, dn_span = _extract_dn(flat)
    hmin, hmax, hspan = _extract_height(flat)

    evidence = []
    if dn_span:
        evidence.append(("BOM Outlet DN", _snippet(flat, dn_span[0], dn_span[1]), final))
    if hspan:
        evidence.append(("BOM Installation height", _snippet(flat, hspan[0], hspan[1]), final))

    return {
        "bom_code": f"ROHBAU-{aid}" if aid else (article or title[:40]),
        "bom_name": title,
        "bom_url": final,
        "article_no": article,
        "outlet_dn": "/".join(dns) if dns else None,
        "height_adj_min_mm": hmin,
        "height_adj_max_mm": hmax,
        "is_default": "yes",
        "evidence_json": json.dumps(evidence, ensure_ascii=False) if evidence else None,
    }


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    _ = params
    src = _canonicalize_url((product_url or "").strip())
    st, final, html, _ = _safe_get_text(src)
    if st != 200 or not html:
        return []

    rohbau_links = _extract_rohbau_links(html, final)
    out: List[Dict[str, Any]] = []
    seen = set()
    for u in rohbau_links[:10]:
        if u in seen:
            continue
        seen.add(u)
        parsed = _parse_rohbauset_page(u)
        if parsed:
            out.append(parsed)

    return out
