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
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    "Connection": "keep-alive",
}

SEED_PAGES = [
    "https://www.buildingdrainage.aco/products/collect/bathroom-drainage/channel/aco-showerdrain-c/aco-showerdrain-c-standard/channel-body-aco-showerdrain-c-installation-height-top-edge-screed-57-128-mm-200-mm",
    "https://www.buildingdrainage.aco/products/collect/bathroom-drainage/channel/aco-showerdrain-eplus/channel/channel-body-with-installation-height-92-140-mm-din-en-1253-1",
]

# fallback for environments where buildingdrainage.aco cannot be reached (proxy/CDN restrictions)
FALLBACK_ITEMS_BY_SEED: Dict[str, List[Tuple[int, str]]] = {
    SEED_PAGES[0]: [
        (685, "9010.85.40"),
        (785, "9010.85.41"),
        (885, "9010.85.42"),
        (985, "9010.85.43"),
        (1185, "9010.85.44"),
    ],
    SEED_PAGES[1]: [
        (685, "9010.88.40"),
        (785, "9010.88.41"),
        (985, "9010.88.43"),
        (1185, "9010.88.44"),
    ],
}

FALLBACK_TEXT_BY_SEED: Dict[str, str] = {
    SEED_PAGES[0]: (
        "ACO ShowerDrain C standard. Flow rate: 0.8 l/s, 1.0 l/s. "
        "Installation height: 57-128 mm. Outlet: ND 40 / ND 50. EN 1253-1."
    ),
    SEED_PAGES[1]: (
        "ACO ShowerDrain E+ channel body. Flow rate: 0.6 l/s, 0.9 l/s, 1.2 l/s. "
        "Installation height: 92-140 mm. Outlet: ND 50. DIN EN 1253-1."
    ),
}

_PAIR_RE = re.compile(r"(\d{3,4})\s*mm\s*([0-9][0-9.]{6,})", re.IGNORECASE)
_FLOW_LPS_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
_HEIGHT_RANGE_RE = re.compile(r"installation\s*height[^\d]{0,30}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm", re.IGNORECASE)
_HEIGHT_SINGLE_RE = re.compile(r"installation\s*height[^\d]{0,30}(\d{2,3})\s*mm", re.IGNORECASE)
_ND_RE = re.compile(r"\bND\s*(\d{2})\b", re.IGNORECASE)


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


def _extract_length_item_pairs(text: str) -> List[Tuple[int, str, str]]:
    out: List[Tuple[int, str, str]] = []
    seen = set()
    flat = _clean_text(text)
    for m in _PAIR_RE.finditer(flat):
        try:
            length_mm = int(m.group(1))
        except Exception:
            continue
        item_no = m.group(2).strip().strip(".,;:")
        item_digits = re.sub(r"\D", "", item_no)
        if len(item_digits) < 6:
            continue
        key = (length_mm, item_no)
        if key in seen:
            continue
        seen.add(key)
        out.append((length_mm, item_no, item_digits))
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

        used_fallback = False
        if st == 200 and html:
            soup = BeautifulSoup(html, "lxml")
            flat = _clean_text(soup.get_text(" ", strip=True) or "")
            title_base = _extract_title(html, final)
            pairs = _extract_length_item_pairs(flat)
        else:
            used_fallback = True
            title_base = seed.rstrip("/").split("/")[-1].replace("-", " ")
            pairs = []
            for length_mm, item_no in FALLBACK_ITEMS_BY_SEED.get(seed, []):
                pairs.append((length_mm, item_no, re.sub(r"\D", "", item_no)))

        kept = 0
        for body_length_mm, item_no, item_digits in pairs:
            nominal_length_mm = body_length_mm + 15
            if not (min_len <= nominal_length_mm <= max_len):
                continue

            kept += 1
            out.append({
                "manufacturer": "aco",
                "product_id": f"aco-{item_digits}",
                "product_family": "ShowerDrain",
                "product_name": f"{title_base} {nominal_length_mm} mm (Item {item_no})",
                "product_url": f"{seed}#item-{item_digits}",
                "sources": seed,
                "candidate_type": "drain",
                "complete_system": "yes",
                "selected_length_mm": want,
                "length_mode": "table_plus_15",
                "length_delta_mm": nominal_length_mm - want,
            })

        debug.append({
            "site": "aco",
            "seed_url": seed,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": kept,
            "method": "seed_fallback" if used_fallback else "seed",
            "is_index": None,
        })

    return out, debug


def _snippet(flat: str, start: int, end: int, pad: int = 70) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _parse_flat_text_into_params(flat: str, source_url: str, res: Dict[str, Any]) -> None:
    # Flow rates (prefer explicit l/s bullet values)
    lps_vals: List[float] = []
    for m in _FLOW_LPS_RE.finditer(flat):
        raw = m.group(1)
        try:
            v = float(raw.replace(",", "."))
        except Exception:
            continue
        if 0.05 <= v <= 5.0:
            lps_vals.append(v)
            res["evidence"].append(("Flow rate option (l/s)", _snippet(flat, m.start(), m.end()), source_url))

    lps_vals = sorted(set(lps_vals))
    if lps_vals:
        res["flow_rate_lps_options"] = json.dumps(lps_vals, ensure_ascii=False)
        res["flow_rate_lps"] = max(lps_vals)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"
    else:
        lps, raw_text, unit, status = select_flow_rate(flat)
        res["flow_rate_lps"] = lps
        res["flow_rate_raw_text"] = raw_text
        res["flow_rate_unit"] = unit
        res["flow_rate_status"] = status

    # Installation height
    m = _HEIGHT_RANGE_RE.search(flat)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if 20 <= lo <= 300 and 20 <= hi <= 300:
            res["height_adj_min_mm"] = lo
            res["height_adj_max_mm"] = hi
            res["evidence"].append(("Installation height (mm)", _snippet(flat, m.start(), m.end()), source_url))
    else:
        m2 = _HEIGHT_SINGLE_RE.search(flat)
        if m2:
            v = int(m2.group(1))
            if 20 <= v <= 300:
                res["height_adj_min_mm"] = v
                res["height_adj_max_mm"] = v
                res["evidence"].append(("Installation height (mm)", _snippet(flat, m2.start(), m2.end()), source_url))

    # Outlet DN options from ND mentions
    dns = sorted({f"DN{int(mm.group(1))}" for mm in _ND_RE.finditer(flat) if mm.group(1) in {"40", "50", "70"}})
    if dns:
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        first = _ND_RE.search(flat)
        if first:
            res["evidence"].append(("Outlet DN", _snippet(flat, first.start(), first.end()), source_url))

    # EN 1253
    m_en = re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat, re.IGNORECASE)
    if m_en:
        res["din_en_1253_cert"] = "yes"
        res["evidence"].append(("DIN EN 1253", _snippet(flat, m_en.start(), m_en.end()), source_url))


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
    if st == 200 and html:
        soup = BeautifulSoup(html, "lxml")
        flat = _clean_text(soup.get_text(" ", strip=True) or "")
        _parse_flat_text_into_params(flat, final, res)
        return res

    # fallback mode for blocked environment
    fallback_text = FALLBACK_TEXT_BY_SEED.get(src)
    if fallback_text:
        flat = _clean_text(fallback_text)
        res["evidence"].append(("Fallback spec", "Using local fallback because source fetch failed", src))
        _parse_flat_text_into_params(flat, src, res)

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return []
