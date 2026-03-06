from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import gzip
import hashlib
import json
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..flowrate import select_flow_rate
from ..pdf_text import extract_pdf_text_from_url


BASE = "https://www.viega.de"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

INCLUDE_KWS = ("duschrinne", "duschablauf", "rinne", "advantix", "duschrinnen")
EXCLUDE_KWS = ("service", "kontakt", "presse", "karriere", "download", "katalog", "video", "news")

LENGTH_RE_LIST = [
    re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE),
    re.compile(r"\b(\d)\.(\d{3})\s*mm\b", re.IGNORECASE),
    re.compile(r"\bl(?:ä|ae)nge\s*(\d{3,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d{3,4})mm\b", re.IGNORECASE),
]

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
LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE | re.DOTALL)


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"


def _safe_get_bytes(url: str, timeout: int = 45) -> Tuple[Optional[int], str, bytes, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.content or b""), ""
    except Exception as e:
        return None, url, b"", f"{type(e).__name__}: {e}"


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _abs(href: str, base: str) -> str:
    return urljoin(base, href or "")


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
    title = soup.select_one("title")
    if title:
        txt = _clean_text(title.get_text(" ", strip=True))
        if txt:
            return txt
    return fallback_url.rstrip("/").split("/")[-1].replace("-", " ")


def _extract_length_mm(text: str) -> Optional[int]:
    src = text or ""
    for rx in LENGTH_RE_LIST:
        m = rx.search(src)
        if not m:
            continue
        try:
            if len(m.groups()) == 2:
                v = int(m.group(1) + m.group(2))
            else:
                v = int(m.group(1))
            if 300 <= v <= 2500:
                return v
        except Exception:
            continue
    return None


def _extract_article_digits(text: str) -> Optional[str]:
    if not text:
        return None
    # common Viega-like article patterns with separators or longer digit chunks
    m = re.search(r"\b(?:\d{2,3}[.\-/]\d{2,3}[.\-/]\d{2,3}|\d{7,10})\b", text)
    if not m:
        return None
    digits = re.sub(r"\D", "", m.group(0))
    if len(digits) < 6:
        return None
    return digits


def _make_product_id(url: str, title: str = "") -> str:
    art = _extract_article_digits(f"{url} {title}")
    if art:
        return f"viega-{art}"
    h = hashlib.sha1((url or "").encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"viega-{h}"


def _robots_sitemaps(base_url: str) -> List[str]:
    robots_url = base_url.rstrip("/") + "/robots.txt"
    st, _, txt, _ = _safe_get_text(robots_url, timeout=25)
    if st != 200 or not txt:
        return [base_url.rstrip("/") + "/sitemap.xml"]

    out: List[str] = []
    for line in txt.splitlines():
        if line.lower().startswith("sitemap:"):
            u = line.split(":", 1)[1].strip()
            if u.startswith("http"):
                out.append(u)
    return list(dict.fromkeys(out)) or [base_url.rstrip("/") + "/sitemap.xml"]


def _extract_sitemap_urls(payload: bytes) -> Tuple[List[str], bool]:
    if not payload:
        return [], False

    if payload[:2] == b"\x1f\x8b":
        try:
            payload = gzip.decompress(payload)
        except Exception:
            return [], False

    txt = payload.decode("utf-8", errors="ignore").strip()
    if not txt:
        return [], False

    if "<" not in txt[:200] and "http" in txt:
        urls = [u.strip() for u in re.split(r"\s+", txt) if u.strip().startswith("http")]
        xmlish = sum(1 for u in urls if u.lower().endswith((".xml", ".xml.gz", ".gz")))
        is_index = (xmlish >= max(1, int(0.6 * len(urls)))) if urls else False
        return urls, is_index

    locs = [m.group(1).strip() for m in LOC_RE.finditer(txt) if m.group(1).strip()]
    if not locs:
        return [], False

    is_index = "<sitemapindex" in txt.lower()
    if not is_index:
        xmlish = sum(1 for u in locs if u.lower().endswith((".xml", ".xml.gz", ".gz")))
        is_index = (xmlish >= max(1, int(0.6 * len(locs))))

    return locs, is_index


def _crawl_sitemaps(start_sitemaps: List[str], max_sitemaps: int = 120, max_pages: int = 80000) -> Tuple[List[str], List[Dict[str, Any]]]:
    seen = set()
    queue = list(start_sitemaps)
    pages: List[str] = []
    debug: List[Dict[str, Any]] = []

    while queue and len(seen) < max_sitemaps and len(pages) < max_pages:
        sm = queue.pop(0)
        if sm in seen:
            continue
        seen.add(sm)

        st, final, body, err = _safe_get_bytes(sm, timeout=45)
        if st != 200 or not body:
            debug.append({"site": "viega", "seed_url": sm, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "sitemap", "is_index": None})
            continue

        urls, is_index = _extract_sitemap_urls(body)
        debug.append({"site": "viega", "seed_url": sm, "status_code": st, "final_url": final, "error": err, "candidates_found": len(urls), "method": "sitemap", "is_index": bool(is_index)})

        if is_index:
            for u in urls:
                if u not in seen:
                    queue.append(u)
        else:
            pages.extend(urls)

    return list(dict.fromkeys(pages)), debug


def _relevant_channel_url(url: str) -> bool:
    u = (url or "").lower()
    if not u.startswith("http"):
        return False
    if any(k in u for k in EXCLUDE_KWS):
        return False
    return any(k in u for k in INCLUDE_KWS)


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _extract_pdf_candidates(html: str, base_url: str) -> List[Tuple[str, int]]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[Tuple[str, int]] = []
    seen = set()

    for a in soup.select("a[href*='.pdf']"):
        href = a.get("href") or ""
        if ".pdf" not in href.lower():
            continue
        url = _abs(href, base_url)
        if url in seen:
            continue
        seen.add(url)
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        score = 0
        if "datenblatt" in txt or "technische daten" in txt or "product" in txt:
            score += 4
        if "montage" in txt or "anleitung" in txt:
            score += 1
        out.append((url, score))

    out.sort(key=lambda x: x[1], reverse=True)
    return out


def _apply_text_extraction(res: Dict[str, Any], flat: str, src: str) -> None:
    if not flat:
        return

    # outlet DN (contextual)
    dns = sorted({f"DN{m.group(1)}" for m in DN_RE.finditer(flat)})
    if dns and res.get("outlet_dn") is None:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        first = DN_RE.search(flat)
        if first:
            res["evidence"].append(("Outlet DN", _snippet(flat, first.start(), first.end()), src))

    # flow options with reduction filter
    lps_vals: List[float] = []
    for m in FLOW_LPS_RE.finditer(flat):
        sn = _snippet(flat, m.start(), m.end(), pad=40).lower()
        prefix = flat[max(0, m.start() - 6):m.start()]
        if FLOW_REJECT_RE.search(sn) or "-" in prefix:
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

    # heights - ignore trap seal contexts
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

    sitemaps = _robots_sitemaps(BASE)
    pages, dbg = _crawl_sitemaps(sitemaps)
    debug.extend(dbg)

    cands = [u for u in pages if _relevant_channel_url(u)]

    # Conservative: first pass length from URL only
    for u in sorted(set(cands)):
        length = _extract_length_mm(u)
        title = u.rstrip("/").split("/")[-1].replace("-", " ")
        if length is None:
            # limited title fetch for unresolved lengths
            st, final, html, err = _safe_get_text(u, timeout=20)
            debug.append({"site": "viega", "seed_url": u, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "title_probe", "is_index": None})
            if st == 200 and html:
                title = _extract_title(html, u)
                length = _extract_length_mm(title)

        if length is None or not (min_len <= length <= max_len):
            continue

        out.append({
            "manufacturer": "viega",
            "product_id": _make_product_id(u, title),
            "product_family": "Advantix",
            "product_name": f"{title} ({length} mm)",
            "product_url": u,
            "sources": u,
            "candidate_type": "drain",
            "complete_system": "yes",
            "selected_length_mm": want,
            "length_mode": "url_or_title",
            "length_delta_mm": length - want,
        })

    # de-dup by URL
    dedup = {}
    for r in out:
        dedup[(r["manufacturer"], r["product_url"])] = r

    debug.append({"site": "viega", "seed_url": BASE + "/sitemap.xml", "status_code": 200 if dedup else None, "final_url": BASE + "/sitemap.xml", "error": "" if dedup else "No candidates after filters.", "candidates_found": len(dedup), "method": "final", "is_index": None})
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

    if re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat, re.IGNORECASE):
        res["din_en_1253_cert"] = "yes"
        m = re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat, re.IGNORECASE)
        if m:
            res["evidence"].append(("DIN EN 1253", _snippet(flat, m.start(), m.end()), final))

    need_pdf = any(res.get(k) is None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"])
    if need_pdf:
        for pdf_url, _score in _extract_pdf_candidates(html, final)[:4]:
            pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
            res["evidence"].append(("PDF status", pdf_status, pdf_url))
            if not pdf_text:
                continue
            _apply_text_extraction(res, _clean_text(pdf_text), pdf_url)
            if re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", pdf_text, re.IGNORECASE) and res.get("din_en_1253_cert") is None:
                res["din_en_1253_cert"] = "yes"

            done = all(res.get(k) is not None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"])
            if done:
                break

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return []
