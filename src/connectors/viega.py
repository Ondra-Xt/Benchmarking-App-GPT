from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple
import json
import re
from urllib.parse import urljoin, urlparse

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

DETAIL_SCOPE = "/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/"
CATALOG_SEED = "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen.html"
DETAIL_SEEDS = [
    "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Cleviva-Duschrinnen/Einbauhoehe-ab-70-mm/Advantix-Cleviva-Duschrinne-4981-11.html",
    "https://www.viega.de/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/Advantix-Vario-Duschrinnen/Einbauhoehe-ab-70-mm/Advantix-Vario-Duschrinnen-4966-10.html",
]

LENGTH_RE_LIST = [
    re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE),
    re.compile(r"\b(\d)\.(\d{3})\s*mm\b", re.IGNORECASE),
    re.compile(r"\bl(?:ä|ae)nge\s*(\d{3,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d{3,4})mm\b", re.IGNORECASE),
]
DETAIL_URL_RE = re.compile(r"-\d{4,5}-\d{2}\.html$", re.IGNORECASE)
ARTICLE_FROM_URL_RE = re.compile(r"-(\d{4,5}-\d{2})\.html(?:$|[?#])", re.IGNORECASE)
# strict DN parsing requires literal DN (prevents 70mm -> DN70 artifacts)
DN_PAIR_RE = re.compile(r"\bDN\s*(40|50)\s*/\s*(40|50)\b", re.IGNORECASE)
DN_SINGLE_RE = re.compile(r"\b(?:Nennweite\s*)?DN\s*(40|50)\b", re.IGNORECASE)
# prefer Ablaufleistung contexts; parse decimal comma/dot only
FLOW_LPS_RE = re.compile(r"(?<!\d)(\d{1,2}(?:[\.,]\d{1,2})?)\s*l/s\b", re.IGNORECASE)
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
VARIABLE_LEN_RE = re.compile(r"\b(vario|variabel|stufenlos|kuerzbar|kürzbar)\b", re.IGNORECASE)


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


def _in_scope(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.netloc.endswith("viega.de") and DETAIL_SCOPE in (p.path or "")
    except Exception:
        return False


def _is_detail_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return _in_scope(url) and bool(DETAIL_URL_RE.search(p.path or ""))
    except Exception:
        return False


def _extract_detail_links_from_catalog(html: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html or "", "lxml")
    out: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        u = _abs(href, base_url)
        if _is_detail_url(u):
            out.add(u)
    return out


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


def _resolve_length_from_text(flat: str) -> Tuple[Optional[int], Optional[str]]:
    if VARIABLE_LEN_RE.search(flat or ""):
        m = VARIABLE_LEN_RE.search(flat or "")
        return None, _snippet(flat, m.start(), m.end()) if m else "variable length"
    opts = _extract_length_options(flat)
    if not opts:
        return None, None
    chosen = max(opts)
    m = re.search(rf"\b{chosen}\s*mm\b|\b{chosen}mm\b|\b{str(chosen)[:1]}\.{str(chosen)[1:]}\s*mm\b", flat, re.IGNORECASE)
    return chosen, (_snippet(flat, m.start(), m.end()) if m else f"{chosen} mm")


def _extract_dns(flat: str) -> Tuple[List[str], Optional[Tuple[int, int]]]:
    dns: List[str] = []
    spans: List[Tuple[int, int]] = []

    for m in DN_PAIR_RE.finditer(flat):
        a = f"DN{m.group(1)}"
        b = f"DN{m.group(2)}"
        if a not in dns:
            dns.append(a)
        if b not in dns:
            dns.append(b)
        spans.append((m.start(), m.end()))

    for m in DN_SINGLE_RE.finditer(flat):
        dn = f"DN{m.group(1)}"
        if dn not in dns:
            dns.append(dn)
            spans.append((m.start(), m.end()))

    dns_sorted = sorted(dns)
    return dns_sorted, (spans[0] if spans else None)


def _extract_flow_vals(flat: str) -> Tuple[List[float], Optional[Tuple[int, int]]]:
    vals: List[float] = []
    first_span: Optional[Tuple[int, int]] = None
    for m in FLOW_LPS_RE.finditer(flat):
        ctx = _snippet(flat, m.start(), m.end(), pad=50)
        if "ablaufleistung" not in ctx.lower() and "abflussleistung" not in ctx.lower():
            continue
        if FLOW_REJECT_RE.search(ctx.lower()):
            continue
        try:
            v = float(m.group(1).replace(",", "."))
        except Exception:
            continue
        if v < 0.10:
            continue
        if v <= 3.0:
            vals.append(v)
            if first_span is None:
                first_span = (m.start(), m.end())
    return sorted(set(vals)), first_span


def _apply_text_extraction(res: Dict[str, Any], flat: str, src: str) -> None:
    if not flat:
        return

    dns, dn_span = _extract_dns(flat)
    if dns and res.get("outlet_dn") is None:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        if dn_span:
            res["evidence"].append(("Outlet DN", _snippet(flat, dn_span[0], dn_span[1]), src))

    flow_opts, flow_span = _extract_flow_vals(flat)
    if flow_opts:
        res["flow_rate_lps_options"] = json.dumps(flow_opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(flow_opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"
        if flow_span:
            res["evidence"].append(("Flow rate option (Ablaufleistung l/s)", _snippet(flat, flow_span[0], flow_span[1]), src))
    elif res.get("flow_rate_lps") is None:
        lps, raw, unit, status = select_flow_rate(flat)
        if lps is not None and lps >= 0.10:
            res["flow_rate_lps"] = lps
            res["flow_rate_raw_text"] = raw
            res["flow_rate_unit"] = unit
            res["flow_rate_status"] = status

    h = HEIGHT_RE.search(flat)
    if h and not TRAP_SEAL_RE.search(flat[h.start():h.end()]):
        a = int(h.group(1))
        b = int(h.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if 20 <= lo <= 300 and 20 <= hi <= 300:
            res["height_adj_min_mm"] = lo
            res["height_adj_max_mm"] = hi
            res["evidence"].append(("Installation height (mm)", _snippet(flat, h.start(), h.end()), src))
    elif res.get("height_adj_min_mm") is None:
        hs = HEIGHT_SINGLE_RE.search(flat)
        if hs and not TRAP_SEAL_RE.search(flat[hs.start():hs.end()]):
            v = int(hs.group(1))
            if 20 <= v <= 300:
                res["height_adj_min_mm"] = v
                res["height_adj_max_mm"] = v
                res["evidence"].append(("Installation height (mm)", _snippet(flat, hs.start(), hs.end()), src))


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    _ = int(tolerance_mm)  # discovery no longer filters by length

    out: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    discovered: Set[str] = set(DETAIL_SEEDS)

    st, final, html, err = _safe_get_text(CATALOG_SEED, timeout=35)
    if st == 200 and html:
        links = _extract_detail_links_from_catalog(html, final)
        discovered.update(links)
        debug.append({"site": "viega", "seed_url": CATALOG_SEED, "status_code": st, "final_url": final, "error": err, "candidates_found": len(links), "method": "catalog", "is_index": None})
    else:
        debug.append({"site": "viega", "seed_url": CATALOG_SEED, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "catalog", "is_index": None})

    for url in sorted(discovered):
        st, final, html, err = _safe_get_text(url, timeout=35)
        title = url.rstrip("/").split("/")[-1].replace("-", " ")
        length = None
        if st == 200 and html:
            title = _extract_title(html, final)
            flat = _main_flat_text(html)
            length, _ = _resolve_length_from_text(f"{title} {flat}")

        row = {
            "manufacturer": "viega",
            "product_id": _product_id_from_url(url),
            "product_family": "Advantix",
            "product_name": title if length is None else f"{title} ({length} mm)",
            "product_url": url,
            "sources": url,
            "candidate_type": "drain",
            "complete_system": "yes",
            "selected_length_mm": want,
            "length_mode": "unknown" if length is None else "html",
            "length_delta_mm": None if length is None else (length - want),
        }
        out.append(row)

        debug.append({
            "site": "viega",
            "seed_url": url,
            "status_code": st,
            "final_url": final,
            "error": err if st != 200 else ("length_unknown" if length is None else ""),
            "candidates_found": 1,
            "method": "detail",
            "is_index": None,
        })

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
        "resolved_length_mm": None,
        "evidence": [],
    }

    src = (product_url or "").strip()
    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))
    if st != 200 or not html:
        return res

    flat = _main_flat_text(html)
    _apply_text_extraction(res, flat, final)

    length, len_snip = _resolve_length_from_text(flat)
    if length is not None:
        res["resolved_length_mm"] = length
        res["evidence"].append(("Resolved length (mm)", len_snip or f"{length} mm", final))
    elif VARIABLE_LEN_RE.search(flat):
        res["evidence"].append(("Resolved length", "variable length", final))

    m_en = re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat, re.IGNORECASE)
    if m_en:
        res["din_en_1253_cert"] = "yes"
        res["evidence"].append(("DIN EN 1253", _snippet(flat, m_en.start(), m_en.end()), final))

    need_pdf = any(res.get(k) is None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm", "resolved_length_mm"])
    if need_pdf:
        for pdf_url, _score in _extract_pdf_candidates(html, final)[:4]:
            pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
            res["evidence"].append(("PDF status", pdf_status, pdf_url))
            if not pdf_text:
                continue
            flat_pdf = _clean_text(pdf_text)
            _apply_text_extraction(res, flat_pdf, pdf_url)

            if res.get("resolved_length_mm") is None:
                plen, psnip = _resolve_length_from_text(flat_pdf)
                if plen is not None:
                    res["resolved_length_mm"] = plen
                    res["evidence"].append(("Resolved length (mm)", psnip or f"{plen} mm", pdf_url))
                elif VARIABLE_LEN_RE.search(flat_pdf):
                    res["evidence"].append(("Resolved length", "variable length", pdf_url))

            if res.get("din_en_1253_cert") is None and re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat_pdf, re.IGNORECASE):
                res["din_en_1253_cert"] = "yes"

            done = all(res.get(k) is not None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm", "resolved_length_mm"])
            if done:
                break

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return []
