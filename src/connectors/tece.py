from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import json
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de,de-DE;q=0.9,en;q=0.8,cs;q=0.7",
    "Connection": "keep-alive",
}

PRODUCT_URL_RE = re.compile(r"https?://[^\s\"'>]*/PR/(\d+)/[^\s\"'>]*index\.xhtml", re.IGNORECASE)
LENGTH_RE = re.compile(r"(\d{3,4})\s*mm", re.IGNORECASE)
HEIGHT_MM_RE = re.compile(r"(\d{1,3})\s*mm", re.IGNORECASE)

SEED_URLS = [
    "https://productdaten.tece.de/PR/601202/index.xhtml",
    "https://productdaten.tece.de/PR/601201/index.xhtml",
    "https://productdaten.tece.de/PR/601200/index.xhtml",
    "https://productdaten.tece.de/PR/671200/index.xhtml",
]

FLOW_SOURCE_URL = "https://www.tece.com/de/entwaesserungstechnik/duschrinne-tecedrainline/ablaeufe-zubehoer"


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _extract_product_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []

    soup = BeautifulSoup(html.replace("\\/", "/"), "lxml")
    links: List[str] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        text = _clean_text(a.get_text(" ", strip=True)).lower()
        is_variant = ("varianten" in text) or ("alle produkte" in text)

        target = urljoin(base_url, href)
        if PRODUCT_URL_RE.search(target):
            links.append(target)
            continue

        if is_variant:
            # variant/index pages can link to more product pages; include for BFS traversal
            links.append(target)

    # regex fallback on raw html
    for m in PRODUCT_URL_RE.finditer(html.replace("\\/", "/")):
        links.append(m.group(0))

    return list(dict.fromkeys(links))


def _extract_title_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for sel in ["h1", "title", "h2"]:
        n = soup.select_one(sel)
        if n:
            txt = _clean_text(n.get_text(" ", strip=True))
            if txt:
                return txt
    return _clean_text(soup.get_text(" ", strip=True)[:400])


def _extract_length_from_text(name: str) -> Optional[int]:
    vals = []
    for m in LENGTH_RE.finditer(name or ""):
        try:
            v = int(m.group(1))
            if 300 <= v <= 3000:
                vals.append(v)
        except Exception:
            continue
    if not vals:
        return None
    return vals[0]


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    min_len = max(0, int(target_length_mm) - int(tolerance_mm))
    max_len = int(target_length_mm) + int(tolerance_mm)

    debug: List[Dict[str, Any]] = []
    queue = list(SEED_URLS)
    seen = set()
    product_pages: List[str] = []

    while queue and len(seen) < 300:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)

        st, final, html, err = _safe_get_text(url, timeout=30)
        debug.append({
            "site": "tece",
            "seed_url": url,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": 0,
            "method": "seed_bfs",
            "is_index": None,
        })
        if st != 200 or not html:
            continue

        # collect PR product pages and variant links
        links = _extract_product_links(html, final)
        for lk in links:
            if PRODUCT_URL_RE.search(lk):
                product_pages.append(lk)
            if "productdaten.tece.de" in lk and lk not in seen and len(queue) < 500:
                queue.append(lk)

        # page itself can be a product page
        if PRODUCT_URL_RE.search(final):
            product_pages.append(final)

    out: List[Dict[str, Any]] = []
    for u in list(dict.fromkeys(product_pages)):
        st, final, html, err = _safe_get_text(u, timeout=25)
        debug.append({
            "site": "tece",
            "seed_url": u,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": 0,
            "method": "product_filter",
            "is_index": None,
        })
        if st != 200 or not html:
            continue

        name = _extract_title_text(html)
        length_mm = _extract_length_from_text(name)
        if length_mm is None or not (min_len <= length_mm <= max_len):
            continue

        out.append({
            "manufacturer": "tece",
            "product_family": "TECEdrain",
            "product_name": name,
            "product_url": final,
            "sources": final,
            "candidate_type": "drain",
            "complete_system": "yes",
            "selected_length_mm": int(target_length_mm),
            "length_mode": "title",
            "length_delta_mm": length_mm - int(target_length_mm),
        })

    return out, debug


def _extract_height_from_product_html(html: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")

    # Find block headed by "Bauhöhe bis OKFF (je nach Ablauf)"
    heading = None
    for n in soup.find_all(["h1", "h2", "h3", "h4", "h5", "strong", "p", "div", "span", "th", "td"]):
        txt = _clean_text(n.get_text(" ", strip=True))
        if "bauhöhe bis okff" in txt.lower() and "ablauf" in txt.lower():
            heading = n
            break

    mm_vals: List[int] = []
    snippet = None

    if heading is not None:
        parts = [_clean_text(heading.get_text(" ", strip=True))]
        # Prefer list items right after heading
        cur = heading
        for _ in range(8):
            cur = cur.find_next()
            if cur is None:
                break
            t = _clean_text(cur.get_text(" ", strip=True))
            if not t:
                continue
            parts.append(t)
            if cur.name in {"li", "td", "p"}:
                for m in HEIGHT_MM_RE.finditer(t):
                    v = int(m.group(1))
                    if 1 <= v <= 300:
                        mm_vals.append(v)
            if len(mm_vals) >= 6:
                break
        snippet = " | ".join(parts)[:350]

    # fallback text segment if list extraction missed
    if not mm_vals:
        flat = _clean_text(soup.get_text(" ", strip=True))
        m = re.search(r"bauhöhe\s+bis\s+okff\s*\(je\s+nach\s+ablauf\)", flat, re.IGNORECASE)
        if m:
            seg = flat[m.end(): min(len(flat), m.end() + 500)]
            snippet = flat[max(0, m.start() - 20): min(len(flat), m.end() + 300)]
            for mm in HEIGHT_MM_RE.finditer(seg):
                v = int(mm.group(1))
                if 1 <= v <= 300:
                    mm_vals.append(v)

    if not mm_vals:
        return None, None, None

    return min(mm_vals), max(mm_vals), snippet


def _extract_flow_options_from_official_page() -> Tuple[Optional[List[float]], Optional[str]]:
    st, final, html, _ = _safe_get_text(FLOW_SOURCE_URL, timeout=35)
    if st != 200 or not html:
        return None, final

    flat = _clean_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))
    vals: List[float] = []
    for m in re.finditer(r"leistung\s*(\d+[\.,]\d+)\s*l\s*/\s*s", flat, re.IGNORECASE):
        try:
            v = float(m.group(1).replace(",", "."))
            if 0.05 <= v <= 5.0:
                vals.append(round(v, 4))
        except Exception:
            continue

    vals = sorted(set(vals))
    if not vals:
        return None, final
    return vals, final


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    _ = product_url, params
    return []


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
        "evidence": [],
    }

    src = (product_url or "").strip()
    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))

    if st == 200 and html:
        hmin, hmax, hsnip = _extract_height_from_product_html(html)
        if hmin is not None and hmax is not None:
            res["height_adj_min_mm"] = hmin
            res["height_adj_max_mm"] = hmax
            if hsnip:
                res["evidence"].append(("Installation/Construction height (mm)", hsnip, final))

    # DN fixed MVP assumption with explicit evidence source as requested
    res["outlet_dn"] = "DN50"
    res["outlet_dn_default"] = "DN50"
    res["outlet_dn_options_json"] = json.dumps(["DN50"], ensure_ascii=False)
    res["evidence"].append(("Outlet DN", "DN50", FLOW_SOURCE_URL))

    # flow options from official TECE page (no guessing)
    opts, flow_src = _extract_flow_options_from_official_page()
    if opts:
        res["flow_rate_lps_options"] = json.dumps(opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "from_official_page"
        res["evidence"].append(("Flow rate options", res["flow_rate_lps_options"], flow_src or FLOW_SOURCE_URL))
        res["evidence"].append(("Flow rate", str(res["flow_rate_lps"]), flow_src or FLOW_SOURCE_URL))

    return res
