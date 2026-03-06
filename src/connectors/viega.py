from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import json
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..flowrate import select_flow_rate
from ..pdf_text import extract_pdf_text_from_url


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

SEED_URLS = [
    "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Cleviva-Duschrinnen/Einbauhoehe-ab-70-mm/Advantix-Cleviva-Duschrinne-4981-11.html",
    "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Vario-Duschrinnen/Einbauhoehe-ab-70-mm/Advantix-Vario-Duschrinnen-4966-10.html",
]

LENGTH_RE_LIST = [
    re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE),
    re.compile(r"\b(\d)\.(\d{3})\s*mm\b", re.IGNORECASE),
    re.compile(r"\bl(?:ä|ae)nge\s*(\d{3,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d{3,4})mm\b", re.IGNORECASE),
]
ARTICLE_FROM_URL_RE = re.compile(r"-(\d{3,5}-\d{2})\.html$", re.IGNORECASE)
DN_RE = re.compile(r"(?:nennweite\s*)?dn\s*(40|50|70)\b", re.IGNORECASE)
FLOW_LPS_RE = re.compile(r"(?P<val>\d+(?:[.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
FLOW_REJECT_RE = re.compile(r"reduziert\s+um|reduziert|reduzieren|reduzierung", re.IGNORECASE)
HEIGHT_RE = re.compile(
    r"(?:einbauh(?:ö|oe)he|bauh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm",
    re.IGNORECASE,
)
HEIGHT_SINGLE_RE = re.compile(
    r"(?:einbauh(?:ö|oe)he|bauh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*mm",
    re.IGNORECASE,
)
TRAP_SEAL_RE = re.compile(r"sperrwasserh(?:ö|oe)he|geruchsverschluss|water\s+seal", re.IGNORECASE)


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


def _extract_title(html: str, fallback_url: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    h1 = soup.select_one("h1")
    if h1:
        txt = _clean_text(h1.get_text(" ", strip=True))
        if txt:
            return txt
    t = soup.select_one("title")
    if t:
        txt = _clean_text(t.get_text(" ", strip=True))
        if txt:
            return txt
    return fallback_url.rstrip("/").split("/")[-1].replace("-", " ")


def _extract_length_options(text: str) -> List[int]:
    out: List[int] = []
    seen = set()
    src = text or ""
    for rx in LENGTH_RE_LIST:
        for m in rx.finditer(src):
            try:
                if len(m.groups()) == 2:
                    v = int(m.group(1) + m.group(2))
                else:
                    v = int(m.group(1))
            except Exception:
                continue
            if 300 <= v <= 2500 and v not in seen:
                seen.add(v)
                out.append(v)
    return sorted(out)


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _product_id_from_url(url: str) -> str:
    m = ARTICLE_FROM_URL_RE.search(url or "")
    if m:
        return f"viega-{_digits_only(m.group(1))}"
    tail = (url or "").rstrip("/").split("/")[-1]
    d = _digits_only(tail)
    if len(d) >= 6:
        return f"viega-{d}"
    return f"viega-{abs(hash(url or ''))}"


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _abs(href: str, base: str) -> str:
    return urljoin(base, href or "")


def _extract_pdf_candidates(html: str, base_url: str) -> List[Tuple[str, int]]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[Tuple[str, int]] = []
    seen = set()

    for a in soup.select("a[href*='.pdf']"):
        href = a.get("href") or ""
        if ".pdf" not in href.lower():
            continue
        u = _abs(href, base_url)
        if u in seen:
            continue
        seen.add(u)
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        score = 0
        if "technische daten" in txt or "datenblatt" in txt:
            score += 4
        if "montage" in txt or "anleitung" in txt:
            score += 1
        out.append((u, score))

    out.sort(key=lambda x: x[1], reverse=True)
    return out


def _apply_text_extraction(res: Dict[str, Any], flat: str, src: str) -> None:
    if not flat:
        return

    # outlet DN
    dns = sorted({f"DN{m.group(1)}" for m in DN_RE.finditer(flat)})
    if dns and res.get("outlet_dn") is None:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        first = DN_RE.search(flat)
        if first:
            res["evidence"].append(("Outlet DN", _snippet(flat, first.start(), first.end()), src))

    # flow l/s options
    lps_vals: List[float] = []
    for m in FLOW_LPS_RE.finditer(flat):
        ctx = _snippet(flat, m.start(), m.end(), pad=40).lower()
        pref = flat[max(0, m.start() - 4):m.start()]
        if FLOW_REJECT_RE.search(ctx) or "-" in pref:
            continue
        try:
            v = float(m.group("val").replace(",", "."))
        except Exception:
            continue
        if 0.05 <= v <= 3.0:
            lps_vals.append(v)
            res["evidence"].append(("Flow rate option (l/s)", _snippet(flat, m.start(), m.end()), src))

    if lps_vals:
        opts = sorted(set(lps_vals))
        res["flow_rate_lps_options"] = json.dumps(opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"
    elif res.get("flow_rate_lps") is None:
        lps, raw, unit, status = select_flow_rate(flat)
        res["flow_rate_lps"] = lps
        res["flow_rate_raw_text"] = raw
        res["flow_rate_unit"] = unit
        res["flow_rate_status"] = status

    # heights excluding trap seal contexts
    h = HEIGHT_RE.search(flat)
    if h and not TRAP_SEAL_RE.search(_snippet(flat, h.start(), h.end(), pad=30)):
        a = int(h.group(1))
        b = int(h.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if 20 <= lo <= 300 and 20 <= hi <= 300:
            res["height_adj_min_mm"] = lo
            res["height_adj_max_mm"] = hi
            res["evidence"].append(("Installation height (mm)", _snippet(flat, h.start(), h.end()), src))
    elif res.get("height_adj_min_mm") is None:
        hs = HEIGHT_SINGLE_RE.search(flat)
        if hs and not TRAP_SEAL_RE.search(_snippet(flat, hs.start(), hs.end(), pad=30)):
            v = int(hs.group(1))
            if 20 <= v <= 300:
                res["height_adj_min_mm"] = v
                res["height_adj_max_mm"] = v
                res["evidence"].append(("Installation height (mm)", _snippet(flat, hs.start(), hs.end()), src))


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    tol = int(tolerance_mm)
    min_len = max(0, want - tol)
    max_len = want + tol

    out: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    for seed in SEED_URLS:
        st, final, html, err = _safe_get_text(seed, timeout=35)
        if st != 200 or not html:
            debug.append({
                "site": "viega",
                "seed_url": seed,
                "status_code": st,
                "final_url": final,
                "error": err,
                "candidates_found": 0,
                "method": "seed",
                "is_index": None,
            })
            continue

        title = _extract_title(html, final)
        flat = _main_flat_text(html)
        lengths = _extract_length_options(f"{title} {flat}")

        kept = 0
        if lengths:
            for length in lengths:
                if not (min_len <= length <= max_len):
                    continue
                kept += 1
                out.append({
                    "manufacturer": "viega",
                    "product_id": _product_id_from_url(seed),
                    "product_family": "Advantix",
                    "product_name": f"{title} ({length} mm)",
                    "product_url": seed,
                    "sources": seed,
                    "candidate_type": "drain",
                    "complete_system": "yes",
                    "selected_length_mm": want,
                    "length_mode": "html",
                    "length_delta_mm": length - want,
                })
        else:
            kept += 1
            out.append({
                "manufacturer": "viega",
                "product_id": _product_id_from_url(seed),
                "product_family": "Advantix",
                "product_name": title,
                "product_url": seed,
                "sources": seed,
                "candidate_type": "drain",
                "complete_system": "yes",
                "selected_length_mm": want,
                "length_mode": "unknown",
                "length_delta_mm": None,
            })

        debug.append({
            "site": "viega",
            "seed_url": seed,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": kept,
            "method": "seed",
            "is_index": None,
        })

    # de-dup by product_id + url + name
    dedup = {}
    for r in out:
        dedup[(r["product_id"], r["product_url"], r["product_name"])] = r

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
        "evidence": [],
    }

    src = (product_url or "").strip()
    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))
    if st != 200 or not html:
        return res

    flat = _main_flat_text(html)
    _apply_text_extraction(res, flat, final)

    m_en = re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat, re.IGNORECASE)
    if m_en:
        res["din_en_1253_cert"] = "yes"
        res["evidence"].append(("DIN EN 1253", _snippet(flat, m_en.start(), m_en.end()), final))

    need_pdf = any(res.get(k) is None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"])
    if need_pdf:
        for pdf_url, _score in _extract_pdf_candidates(html, final)[:4]:
            pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
            res["evidence"].append(("PDF status", pdf_status, pdf_url))
            if not pdf_text:
                continue
            _apply_text_extraction(res, _clean_text(pdf_text), pdf_url)
            if res.get("din_en_1253_cert") is None and re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", pdf_text, re.IGNORECASE):
                res["din_en_1253_cert"] = "yes"

            done = all(res.get(k) is not None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"])
            if done:
                break

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return []
