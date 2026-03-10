from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
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

BASE = "https://www.aco-haustechnik.de"
DUSCHRINNEN_SCOPE = "/produkte/badentwaesserung/duschrinnen/"
SEED_PAGES = [
    f"{BASE}{DUSCHRINNEN_SCOPE}",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-c/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-eplus/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-mplus/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-splus/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/",
]

ARTICLE_RE = re.compile(r"\b(?:\d{4}\.?\d{2}\.?\d{2}|\d{8})\b")
L1_RE = re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE)
FLOW_LPS_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
HEIGHT_OE_RE = re.compile(
    r"einbauh(?:ö|oe)he[^.]{0,80}oberkante\s+estrich[^\d]{0,20}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm",
    re.IGNORECASE,
)
HEIGHT_RE = re.compile(r"einbauh(?:ö|oe)he[^\d]{0,25}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm", re.IGNORECASE)
DN_RE = re.compile(r"\bDN\s*(\d{2})\b", re.IGNORECASE)
EN1253_RE = re.compile(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", re.IGNORECASE)
DN_CONTEXT_RE = re.compile(r"ablaufstutzen|ablauf|anschluss|stutzen|\bdn\b", re.IGNORECASE)
FLOW_REJECT_RE = re.compile(r"reduziert|reduzieren|reduziert\s+die\s+abflussleistung", re.IGNORECASE)
ABFLUSS_PREF_RE = re.compile(r"abflusswert|ablaufleistung", re.IGNORECASE)
CATEGORY_PATHS_EXACT = {
    DUSCHRINNEN_SCOPE.rstrip("/"),
    f"{DUSCHRINNEN_SCOPE}aco-showerdrain-b".rstrip("/"),
    f"{DUSCHRINNEN_SCOPE}aco-showerdrain-c".rstrip("/"),
    f"{DUSCHRINNEN_SCOPE}aco-showerdrain-eplus".rstrip("/"),
    f"{DUSCHRINNEN_SCOPE}aco-showerdrain-mplus".rstrip("/"),
    f"{DUSCHRINNEN_SCOPE}aco-showerdrain-splus".rstrip("/"),
}


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _main_flat_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    main = soup.select_one("main")
    if main is not None:
        return _clean_text(main.get_text(" ", strip=True) or "")

    for sel in ["header", "nav", "footer", "aside"]:
        for tag in soup.select(sel):
            tag.decompose()
    return _clean_text(soup.get_text(" ", strip=True) or "")


def _extract_title(html: str, fallback_url: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    h1 = soup.select_one("h1")
    if h1:
        t = _clean_text(h1.get_text(" ", strip=True))
        if t:
            return t
    title = soup.select_one("title")
    if title:
        t = _clean_text(title.get_text(" ", strip=True))
        if t:
            return t
    return fallback_url.rstrip("/").split("/")[-1].replace("-", " ")


def _digits_only(article_no: str) -> str:
    return re.sub(r"\D", "", article_no or "")


def _nominal_length_from_l1(l1_mm: int) -> int:
    return (l1_mm + 15) if (l1_mm % 100 == 85) else l1_mm


def _extract_pairs_from_table(html: str) -> List[Tuple[int, str, str]]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[Tuple[int, str, str]] = []
    seen = set()

    for table in soup.select("table"):
        headers = [_clean_text(th.get_text(" ", strip=True)).lower() for th in table.select("thead th, tr th")]
        header_text = " | ".join(headers)
        if not any(k in header_text for k in ["artikel", "artikel-nr", "artikel nr", "abmessung", "abmessungen", "l1"]):
            continue

        for tr in table.select("tr"):
            cells = tr.select("th, td")
            if not cells:
                continue
            row_texts = [_clean_text(c.get_text(" ", strip=True)) for c in cells]
            row_joined = " | ".join(row_texts)
            l1_match = L1_RE.search(row_joined)
            article_match = ARTICLE_RE.search(row_joined)
            if not l1_match or not article_match:
                continue
            try:
                l1_mm = int(l1_match.group(1))
            except Exception:
                continue
            article_no = article_match.group(0)
            article_digits = _digits_only(article_no)
            if len(article_digits) < 6:
                continue
            key = (l1_mm, article_digits)
            if key in seen:
                continue
            seen.add(key)
            out.append((l1_mm, article_no, article_digits))

    return out


def _extract_pairs_from_flat_text(flat: str) -> List[Tuple[int, str, str]]:
    # fallback parser from flattened text (still real HTML-based; no local fabricated data)
    out: List[Tuple[int, str, str]] = []
    seen = set()
    for m in re.finditer(r"(\d{3,4})\s*mm[^\n]{0,80}?(\d{4}\.?\d{2}\.?\d{2}|\d{8})", flat, re.IGNORECASE):
        try:
            l1_mm = int(m.group(1))
        except Exception:
            continue
        article_no = m.group(2)
        article_digits = _digits_only(article_no)
        if len(article_digits) < 6:
            continue
        key = (l1_mm, article_digits)
        if key in seen:
            continue
        seen.add(key)
        out.append((l1_mm, article_no, article_digits))
    return out




def _extract_primary_article_and_length(flat: str) -> Tuple[Optional[str], Optional[int]]:
    article_digits: Optional[str] = None
    am = ARTICLE_RE.search(flat or "")
    if am:
        d = _digits_only(am.group(0))
        if len(d) >= 6:
            article_digits = d

    length_mm: Optional[int] = None
    lm = L1_RE.search(flat or "")
    if lm:
        try:
            raw = int(lm.group(1))
            length_mm = _nominal_length_from_l1(raw)
        except Exception:
            length_mm = None

    return article_digits, length_mm

def _abs(href: str, base: str) -> str:
    return urljoin(base, href or "")


def _in_scope(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.netloc.endswith("aco-haustechnik.de") and (p.path or "").startswith(DUSCHRINNEN_SCOPE)
    except Exception:
        return False


def _is_accessory_page(url: str, title: str = "") -> bool:
    txt = f"{url} {title}".lower()
    return any(k in txt for k in ("zubehoer", "zubehör", "rost", "abdeckung", "rahmen", "designrost", "rahmenprofil"))


def _is_channel_body_page(url: str, title: str = "") -> bool:
    txt = f"{url} {title}".lower()
    return any(k in txt for k in ("rinnenkoerper", "rinnenkörper", "duschrinne", "showerdrain", "einbauhoehe", "einbauhöhe"))





def _canonicalize_url(url: str) -> str:
    try:
        p = urlparse((url or "").strip())
        path = (p.path or "/").replace("//", "/")
        if path != "/":
            path = path.rstrip("/") + "/"
        return f"{p.scheme}://{p.netloc}{path}"
    except Exception:
        return (url or "").split("#", 1)[0].split("?", 1)[0].rstrip("/") + "/"


def _is_category_page(url: str) -> bool:
    try:
        path = (urlparse(url).path or "").rstrip("/")
        return path in CATEGORY_PATHS_EXACT
    except Exception:
        return False


def _looks_like_detail_drain_page(url: str, html: str) -> bool:
    if "rinnenkoerper" in (url or "").lower():
        return True
    pairs = _extract_pairs_from_table(html)
    if pairs:
        return True
    flat = _main_flat_text_from_html(html)
    has_h = bool(HEIGHT_OE_RE.search(flat) or HEIGHT_RE.search(flat))
    has_dn = any(m.group(1) in {"40", "50", "70"} and _has_dn_context(flat, m.start(), m.end()) for m in DN_RE.finditer(flat))
    has_flow = any(ABFLUSS_PREF_RE.search(_snippet(flat, m.start(), m.end(), pad=70)) for m in FLOW_LPS_RE.finditer(flat))
    return has_h and has_dn and has_flow

def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    tol = int(tolerance_mm)
    min_len = max(0, want - tol)
    max_len = want + tol

    out: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    queue: List[str] = list(SEED_PAGES)
    seen_pages = set()
    detail_pages = set()
    canonical_seen = set()
    dropped_fragments = 0

    # Crawl category/list pages in scope to discover more ranges and detail pages
    while queue and len(seen_pages) < 250:
        page = queue.pop(0)
        if page in seen_pages:
            continue
        seen_pages.add(page)

        st, final, html, err = _safe_get_text(page, timeout=35)
        if st != 200 or not html:
            debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "crawl", "is_index": None})
            continue

        final_c = _canonicalize_url(final)
        if final_c != final:
            dropped_fragments += 1
        if final_c not in canonical_seen:
            canonical_seen.add(final_c)
            detail_pages.add(final_c)

        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            cand = _abs(a.get("href") or "", final)
            if not _in_scope(cand):
                continue
            cand_c = _canonicalize_url(cand)
            if cand_c != cand:
                dropped_fragments += 1
            if cand_c not in seen_pages and cand_c not in queue:
                queue.append(cand_c)
            detail_pages.add(cand_c)

    kept_total = 0
    seen_ids = set()
    product_urls: List[str] = []
    component_urls: List[str] = []
    dropped_category_pages = 0

    for page in sorted(detail_pages):
        # reject category/landing pages from candidates
        if _is_category_page(page):
            dropped_category_pages += 1
            debug.append({"site": "aco", "seed_url": page, "status_code": 200, "final_url": page, "error": "dropped_category_page", "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        st, final, html, err = _safe_get_text(page, timeout=35)
        if st != 200 or not html:
            debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        final_c = _canonicalize_url(final)
        title_base = _extract_title(html, final_c)

        # route candidate type
        if _is_accessory_page(final_c, title_base):
            cand_type = "component"
        elif _looks_like_detail_drain_page(final_c, html):
            cand_type = "drain"
        else:
            debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final_c, "error": "dropped_overview_page", "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        pairs = _extract_pairs_from_table(html)
        if not pairs:
            pairs = _extract_pairs_from_flat_text(_main_flat_text_from_html(html))
        method = "table" if pairs else "detail_only"

        kept = 0
        if cand_type == "drain" and pairs:
            for l1_mm, article_no, article_digits in pairs:
                nominal_length_mm = _nominal_length_from_l1(l1_mm)
                if not (min_len <= nominal_length_mm <= max_len):
                    continue
                pid = f"aco-{article_digits}"
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                kept += 1
                kept_total += 1
                out.append({
                    "manufacturer": "aco",
                    "product_id": pid,
                    "product_family": "ShowerDrain",
                    "product_name": f"{title_base} {nominal_length_mm} mm (Artikel-Nr. {article_no})",
                    "product_url": final_c,
                    "sources": final_c,
                    "candidate_type": "drain",
                    "complete_system": "yes",
                    "selected_length_mm": want,
                    "length_mode": "L1_nominal_heuristic",
                    "length_delta_mm": nominal_length_mm - want,
                })
                product_urls.append(final_c)
        elif cand_type == "drain" and _is_channel_body_page(final_c, title_base):
            flat = _main_flat_text_from_html(html)
            article_digits, length_mm = _extract_primary_article_and_length(flat)
            pid = f"aco-{article_digits}" if article_digits else f"aco-{abs(hash(final_c))}"
            if pid not in seen_ids:
                if length_mm is not None and not (min_len <= length_mm <= max_len):
                    debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final_c, "error": "filtered_by_target_length", "candidates_found": 0, "method": "detail_only", "is_index": None})
                    continue
                seen_ids.add(pid)
                kept += 1
                kept_total += 1
                pname = title_base if length_mm is None else f"{title_base} {length_mm} mm"
                out.append({
                    "manufacturer": "aco",
                    "product_id": pid,
                    "product_family": "ShowerDrain",
                    "product_name": pname,
                    "product_url": final_c,
                    "sources": final_c,
                    "candidate_type": "drain",
                    "complete_system": "yes",
                    "selected_length_mm": want,
                    "length_mode": "unknown" if length_mm is None else "text_nominal",
                    "length_delta_mm": None if length_mm is None else (length_mm - want),
                })
                product_urls.append(final_c)
        elif cand_type == "component":
            pid = f"aco-comp-{abs(hash(final_c))}"
            if pid not in seen_ids:
                seen_ids.add(pid)
                kept += 1
                kept_total += 1
                out.append({
                    "manufacturer": "aco",
                    "product_id": pid,
                    "product_family": "ShowerDrain",
                    "product_name": title_base,
                    "product_url": final_c,
                    "sources": final_c,
                    "candidate_type": "component",
                    "complete_system": "component",
                    "selected_length_mm": want,
                    "length_mode": "unknown",
                    "length_delta_mm": None,
                })
                component_urls.append(final_c)

        debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final_c, "error": err, "candidates_found": kept, "method": method, "is_index": None})

    debug.append({
        "site": "aco",
        "seed_url": f"{BASE}{DUSCHRINNEN_SCOPE}",
        "status_code": 200 if out else None,
        "final_url": f"{BASE}{DUSCHRINNEN_SCOPE}",
        "error": "" if out else "No accepted candidates.",
        "candidates_found": len(out),
        "method": "summary",
        "is_index": None,
        "products_count": sum(1 for r in out if str(r.get("candidate_type")) == "drain"),
        "components_count": sum(1 for r in out if str(r.get("candidate_type")) == "component"),
        "sample_products_urls": json.dumps(product_urls[:10], ensure_ascii=False),
        "sample_components_urls": json.dumps(component_urls[:10], ensure_ascii=False),
        "total_urls": len(detail_pages),
        "after_canonicalize": len(canonical_seen),
        "dropped_fragments": dropped_fragments,
        "dropped_category_pages": dropped_category_pages,
        "accepted_products": sum(1 for r in out if str(r.get("candidate_type")) == "drain"),
        "accepted_components": sum(1 for r in out if str(r.get("candidate_type")) == "component"),
    })

    return out, debug


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _has_dn_context(flat: str, start: int, end: int, window: int = 60) -> bool:
    lo = max(0, start - window)
    hi = min(len(flat), end + window)
    return bool(DN_CONTEXT_RE.search(flat[lo:hi]))


def _is_valid_flow_context(flat: str, start: int, end: int) -> bool:
    # look mostly at nearby prefix where reduction phrasing appears
    prefix = flat[max(0, start - 45):start].lower()
    if FLOW_REJECT_RE.search(prefix):
        return False
    # reject phrasing like "um 0,12 l/s"
    short_prefix = flat[max(0, start - 6):start].lower()
    if short_prefix.endswith(" um ") or short_prefix.endswith("um "):
        return False
    # reject leading minus sign right before number (e.g. "-0,09 l/s")
    lead = flat[max(0, start - 3):start]
    if "-" in lead:
        return False
    return True


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

    src = (product_url or "").split("#", 1)[0].strip()
    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))
    if st != 200 or not html:
        return res

    flat = _main_flat_text_from_html(html)

    # height (prefer Oberkante Estrich phrase)
    hm = HEIGHT_OE_RE.search(flat) or HEIGHT_RE.search(flat)
    if hm:
        a = int(hm.group(1))
        b = int(hm.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if 20 <= lo <= 300 and 20 <= hi <= 300:
            res["height_adj_min_mm"] = lo
            res["height_adj_max_mm"] = hi
            res["evidence"].append(("Einbauhöhe (mm)", _snippet(flat, hm.start(), hm.end()), final))

    # DN
    dns = sorted(
        {
            f"DN{int(m.group(1))}"
            for m in DN_RE.finditer(flat)
            if m.group(1) in {"40", "50", "70"} and _has_dn_context(flat, m.start(), m.end())
        }
    )
    if dns:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        first = DN_RE.search(flat)
        if first:
            res["evidence"].append(("Outlet DN", _snippet(flat, first.start(), first.end()), final))

    # flow rate options from Abflusswert/Ablaufleistung snippets only
    lps_values: List[float] = []
    for m in FLOW_LPS_RE.finditer(flat):
        if not _is_valid_flow_context(flat, m.start(), m.end()):
            continue
        ctx = _snippet(flat, m.start(), m.end(), pad=70)
        if not ABFLUSS_PREF_RE.search(ctx):
            continue
        try:
            v = float(m.group(1).replace(",", "."))
        except Exception:
            continue
        if 0.10 <= v <= 3.0:
            lps_values.append(v)
            res["evidence"].append(("Flow rate option (Abflusswert l/s)", _snippet(flat, m.start(), m.end()), final))

    if lps_values:
        opts = sorted(set(lps_values))
        res["flow_rate_lps_options"] = json.dumps(opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"

    enm = EN1253_RE.search(flat)
    if enm:
        res["din_en_1253_cert"] = "yes"
        res["evidence"].append(("DIN EN 1253", _snippet(flat, enm.start(), enm.end()), final))

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return []
