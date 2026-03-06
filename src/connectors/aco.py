from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import json
import re

import requests
from bs4 import BeautifulSoup

from ..flowrate import select_flow_rate


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

SEED_PAGES = [
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/",
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


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


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


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    tol = int(tolerance_mm)
    min_len = max(0, want - tol)
    max_len = want + tol

    out: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    for seed in SEED_PAGES:
        st, final, html, err = _safe_get_text(seed, timeout=35)
        if st != 200 or not html:
            debug.append({
                "site": "aco",
                "seed_url": seed,
                "status_code": st,
                "final_url": final,
                "error": err,
                "candidates_found": 0,
                "method": "seed",
                "is_index": None,
            })
            continue

        title_base = _extract_title(html, final)
        pairs = _extract_pairs_from_table(html)
        method = "table"
        if not pairs:
            soup = BeautifulSoup(html, "lxml")
            flat = _clean_text(soup.get_text(" ", strip=True) or "")
            pairs = _extract_pairs_from_flat_text(flat)
            method = "text"

        kept = 0
        for l1_mm, article_no, article_digits in pairs:
            nominal_length_mm = _nominal_length_from_l1(l1_mm)
            if not (min_len <= nominal_length_mm <= max_len):
                continue
            kept += 1
            out.append({
                "manufacturer": "aco",
                "product_id": f"aco-{article_digits}",
                "product_family": "ShowerDrain",
                "product_name": f"{title_base} {nominal_length_mm} mm (Artikel-Nr. {article_no})",
                "product_url": seed,
                "sources": seed,
                "candidate_type": "drain",
                "complete_system": "yes",
                "selected_length_mm": want,
                "length_mode": "L1_nominal_heuristic",
                "length_delta_mm": nominal_length_mm - want,
            })

        debug.append({
            "site": "aco",
            "seed_url": seed,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": kept,
            "method": method,
            "is_index": None,
        })

    return out, debug


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


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

    soup = BeautifulSoup(html, "lxml")
    flat = _clean_text(soup.get_text(" ", strip=True) or "")

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
    dns = sorted({f"DN{int(m.group(1))}" for m in DN_RE.finditer(flat) if m.group(1) in {"40", "50", "70"}})
    if dns:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        first = DN_RE.search(flat)
        if first:
            res["evidence"].append(("Outlet DN", _snippet(flat, first.start(), first.end()), final))

    # flow rate options
    lps_values: List[float] = []
    for m in FLOW_LPS_RE.finditer(flat):
        try:
            v = float(m.group(1).replace(",", "."))
        except Exception:
            continue
        if 0.05 <= v <= 5.0:
            lps_values.append(v)
            res["evidence"].append(("Flow rate option (l/s)", _snippet(flat, m.start(), m.end()), final))

    if lps_values:
        opts = sorted(set(lps_values))
        res["flow_rate_lps_options"] = json.dumps(opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"
    else:
        lps, raw_text, unit, status = select_flow_rate(flat)
        res["flow_rate_lps"] = lps
        res["flow_rate_raw_text"] = raw_text
        res["flow_rate_unit"] = unit
        res["flow_rate_status"] = status

    enm = EN1253_RE.search(flat)
    if enm:
        res["din_en_1253_cert"] = "yes"
        res["evidence"].append(("DIN EN 1253", _snippet(flat, enm.start(), enm.end()), final))

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return []
