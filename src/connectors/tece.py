"""TECE connector (full MVP implementation)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import json
import gzip
import re
from urllib.parse import unquote, urljoin, urlparse
import hashlib

import requests
from bs4 import BeautifulSoup

from ..flowrate import select_flow_rate
from ..pdf_text import extract_pdf_text_from_url


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de,de-DE;q=0.9,en;q=0.8,cs;q=0.7",
    "Connection": "keep-alive",
}

PR_PATH_RE = re.compile(r"/PR/(\d+)/index\.xhtml(?:;jsessionid=[^/?#]+)?$", re.IGNORECASE)
PR_LINK_RE = re.compile(r"https?://[^\s\"'>]*/PR/\d+/index\.xhtml(?:;jsessionid=[^\s\"'>]+)?", re.IGNORECASE)
LENGTH_RE = re.compile(r"(?:\b(?:l|länge)\s*[=:]?\s*)?(\d{1,2}(?:\.\d{3})|\d{3,4})\s*mm\b", re.IGNORECASE)
MM_RE = re.compile(r"(\d{1,3})\s*mm", re.IGNORECASE)
TECE_COM_BASE = "https://www.tece.com"
TECE_INCLUDE = ("entwaesserungstechnik", "dusch", "drain", "drainline", "drainprofile")
TECE_EXCLUDE = ("academy", "service", "servicios", "dokumente", "download", "presse", "magazin", "montage", "anleitung", "instruk", "instruction", "manual", "datenblatt", "zubehoer", ".pdf")
PRODUCT_HINTS = ("tecedrainline", "tecedrainprofile", "duschrinne", "duschprofil")
_LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE | re.DOTALL)


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"




def _canonicalize_url(url: str) -> str:
    """Drop query and ;jsessionid to avoid duplicate PR URLs."""
    try:
        p = urlparse(url or "")
    except Exception:
        return url

    params = p.params or ""
    if params.lower().startswith("jsessionid="):
        params = ""

    path = p.path or ""
    path = re.sub(r";jsessionid=[^/?#]+", "", path, flags=re.IGNORECASE)

    return p._replace(path=path, params=params, query="", fragment="").geturl()


def _safe_get_bytes(url: str, timeout: int = 45) -> Tuple[Optional[int], str, bytes, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.content or b""), ""
    except Exception as e:
        return None, url, b"", f"{type(e).__name__}: {e}"


def _robots_sitemaps(base_url: str) -> List[str]:
    st, _, txt, _ = _safe_get_text(base_url.rstrip("/") + "/robots.txt", timeout=25)
    if st != 200 or not txt:
        return [base_url.rstrip("/") + "/sitemap.xml"]

    out: List[str] = []
    for line in txt.splitlines():
        if line.lower().startswith("sitemap:"):
            u = line.split(":", 1)[1].strip()
            if u.startswith("http"):
                out.append(u)
    out = list(dict.fromkeys(out))
    return out or [base_url.rstrip("/") + "/sitemap.xml"]


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

    locs = [m.group(1).strip() for m in _LOC_RE.finditer(txt) if m.group(1).strip()]
    if not locs:
        return [], False

    is_index = "<sitemapindex" in txt.lower()
    if not is_index:
        xmlish = sum(1 for u in locs if u.lower().endswith((".xml", ".xml.gz", ".gz")))
        is_index = xmlish >= max(1, int(0.6 * len(locs)))

    return locs, is_index


def _crawl_sitemaps(start_sitemaps: List[str], max_sitemaps: int = 200, max_pages: int = 300000) -> Tuple[List[str], List[Dict[str, Any]]]:
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
            debug.append({"site": "tece", "seed_url": sm, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "sitemap", "is_index": None})
            continue

        urls, is_index = _extract_sitemap_urls(body)
        debug.append({"site": "tece", "seed_url": sm, "status_code": st, "final_url": final, "error": err, "candidates_found": len(urls), "method": "sitemap", "is_index": bool(is_index)})

        if is_index:
            for u in urls:
                if u not in seen:
                    queue.append(u)
        else:
            pages.extend(urls)

    return list(dict.fromkeys(pages)), debug
def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _is_allowed_tece_url(url: str) -> bool:
    return _is_tececom_de_html(url) and _passes_include_exclude(url)


def _is_pr_product_page(url: str) -> bool:
    # kept for compatibility; tece.com discovery no longer relies on produktdaten PR paths
    return _is_allowed_tece_url(url)


def _extract_pr_number(url: str) -> Optional[str]:
    m = re.search(r"\b(\d{6})\b", unquote(url or ""))
    return m.group(1) if m else None


def _extract_title_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for sel in ["h1", "title", "h2"]:
        n = soup.select_one(sel)
        if n:
            txt = _clean_text(n.get_text(" ", strip=True))
            if txt:
                return txt
    return _clean_text(soup.get_text(" ", strip=True)[:500])


def _extract_product_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []

    soup = BeautifulSoup(html.replace("\/", "/"), "lxml")
    out: List[str] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        target = _canonicalize_url(urljoin(base_url, href))
        if _is_allowed_tece_url(target):
            out.append(target)

    return list(dict.fromkeys(out))


def parse_length_mm(text: str) -> Optional[int]:
    txt = unquote(text or "")
    if not txt:
        return None

    # Normalize German thousands separator in lengths, e.g., 1.200 mm -> 1200 mm.
    txt = re.sub(r"(?<=\d)\.(?=\d{3}\b)", "", txt)

    vals: List[int] = []
    for m in LENGTH_RE.finditer(txt):
        raw = (m.group(1) or "").replace(".", "")
        try:
            v = int(raw)
            if 300 <= v <= 3000:
                vals.append(v)
        except Exception:
            continue

    return vals[0] if vals else None


def _extract_length_from_url(url: str) -> Optional[int]:
    return parse_length_mm(urlparse(url or "").path or "")


def _extract_length_from_text(name: str) -> Optional[int]:
    return parse_length_mm(name)


def _is_tececom_de_html(url: str) -> bool:
    try:
        p = urlparse(url or "")
    except Exception:
        return False
    if p.netloc.lower() != "www.tece.com":
        return False
    path = unquote(p.path or "").lower()
    if not path.startswith("/de/"):
        return False
    if path.endswith(".pdf"):
        return False
    return True


def _passes_include_exclude(url: str) -> bool:
    path = unquote(urlparse(url or "").path or "").lower()
    if not any(k in path for k in TECE_INCLUDE):
        return False
    if any(k in path for k in TECE_EXCLUDE):
        return False
    return True


def _extract_heading_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    title = _clean_text((soup.title.get_text(" ", strip=True) if soup.title else ""))
    h1n = soup.select_one("h1")
    h1 = _clean_text((h1n.get_text(" ", strip=True) if h1n else ""))
    return _clean_text(f"{title} {h1}")


def _is_product_like_heading(txt: str) -> bool:
    t = (txt or "").lower()
    return any(h in t for h in PRODUCT_HINTS)


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    min_len = max(0, int(target_length_mm) - int(tolerance_mm))
    max_len = int(target_length_mm) + int(tolerance_mm)

    start_sitemaps = _robots_sitemaps(TECE_COM_BASE)
    all_urls, debug = _crawl_sitemaps(start_sitemaps)

    total_crawled = len(all_urls)
    de_urls = [_canonicalize_url(u) for u in all_urls if _is_tececom_de_html(_canonicalize_url(u))]
    de_urls = list(dict.fromkeys(de_urls))
    after_de_filter = len(de_urls)

    scoped_urls = [_canonicalize_url(u) for u in de_urls if _passes_include_exclude(_canonicalize_url(u))]
    scoped_urls = list(dict.fromkeys(scoped_urls))
    after_include_exclude = len(scoped_urls)

    out: List[Dict[str, Any]] = []
    after_length_filter = 0
    sample_before_length_filter = scoped_urls[:10]
    sample_dropped_by_length: List[Dict[str, Any]] = []

    for u in scoped_urls:
        if len(out) >= 300:
            break

        st, final, html, err = _safe_get_text(u, timeout=25)
        final_c = _canonicalize_url(final)
        if st != 200 or not html or not _is_tececom_de_html(final_c):
            debug.append({
                "site": "tece",
                "seed_url": u,
                "status_code": st,
                "final_url": final_c,
                "error": err,
                "candidates_found": 0,
                "candidates_accepted": 0,
                "method": "product_filter",
                "is_index": None,
            })
            continue

        heading = _extract_heading_text(html)
        if not _is_product_like_heading(heading):
            continue

        length_mm = _extract_length_from_text(unquote(final_c)) or _extract_length_from_text(heading)
        if length_mm is None:
            full_text = _clean_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))
            length_mm = parse_length_mm(full_text)
            if length_mm is None:
                target_pat = rf"(?:\b{int(target_length_mm)}\s*mm\b|\b{str(int(target_length_mm))[0]}\.{str(int(target_length_mm))[1:]}\s*mm\b|\b{int(target_length_mm)}mm\b)"
                if re.search(target_pat, full_text, re.IGNORECASE):
                    length_mm = int(target_length_mm)

        if length_mm is None or not (min_len <= length_mm <= max_len):
            if len(sample_dropped_by_length) < 10:
                sample_dropped_by_length.append({"url": final_c, "length_mm": length_mm})
            continue
        after_length_filter += 1

        article_m = re.search(r"\b(\d{6})\b", heading)
        product_id = f"tece-{article_m.group(1)}" if article_m else f"tece-{hashlib.sha1(final_c.encode('utf-8')).hexdigest()[:12]}"

        out.append({
            "manufacturer": "tece",
            "product_id": product_id,
            "product_family": "TECEdrain",
            "product_name": heading or final_c,
            "product_url": final_c,
            "sources": final_c,
            "candidate_type": "drain",
            "complete_system": "yes",
            "selected_length_mm": int(target_length_mm),
            "length_mode": "title",
            "length_delta_mm": length_mm - int(target_length_mm),
        })

    debug.append({
        "site": "tece",
        "seed_url": "summary",
        "status_code": 200,
        "final_url": TECE_COM_BASE,
        "error": "",
        "total_crawled": total_crawled,
        "after_de_filter": after_de_filter,
        "after_include_exclude": after_include_exclude,
        "after_length_filter": after_length_filter,
        "sample_before_length_filter": json.dumps(sample_before_length_filter, ensure_ascii=False),
        "sample_dropped_by_length": json.dumps(sample_dropped_by_length, ensure_ascii=False),
        "final_count": len(out),
        "candidates_found": len(scoped_urls),
        "candidates_accepted": len(out),
        "method": "final",
        "is_index": None,
    })

    return out, debug


def _extract_height_from_product_html(html: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")

    heading = None
    for n in soup.find_all(["h1", "h2", "h3", "h4", "h5", "strong", "p", "div", "span", "th", "td"]):
        txt = _clean_text(n.get_text(" ", strip=True)).lower()
        if "bauhöhe" in txt and "okff" in txt:
            heading = n
            break

    mm_vals: List[int] = []
    snippet = None

    if heading is not None:
        parts = [_clean_text(heading.get_text(" ", strip=True))]
        cur = heading
        for _ in range(14):
            cur = cur.find_next()
            if cur is None:
                break
            t = _clean_text(cur.get_text(" ", strip=True))
            if not t:
                continue
            parts.append(t)
            for m in MM_RE.finditer(t):
                v = int(m.group(1))
                if 1 <= v <= 300:
                    mm_vals.append(v)
            if len(mm_vals) >= 8:
                break
        snippet = " | ".join(parts)[:420]

    if not mm_vals:
        flat = _clean_text(soup.get_text(" ", strip=True))
        m = re.search(r"bauhöhe[^\.;:]{0,120}okff[^\.;:]{0,80}", flat, re.IGNORECASE)
        if m:
            seg = flat[m.end(): min(len(flat), m.end() + 550)]
            snippet = flat[max(0, m.start() - 20): min(len(flat), m.end() + 320)]
            for mm in MM_RE.finditer(seg):
                v = int(mm.group(1))
                if 1 <= v <= 300:
                    mm_vals.append(v)

    if not mm_vals:
        return None, None, None
    return min(mm_vals), max(mm_vals), snippet


def _find_datasheet_pdf_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    soup = BeautifulSoup(html.replace("\\/", "/"), "lxml")
    out: List[str] = []

    for a in soup.select("a[href*='.pdf']"):
        href = (a.get("href") or "").strip()
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        target = _canonicalize_url(urljoin(base_url, href))
        if ".pdf" not in target.lower():
            continue
        if any(k in txt for k in ["produktdaten", "datenblatt", "datasheet", "technical"]):
            out.append(target)
        else:
            out.append(target)

    return list(dict.fromkeys(out))


def _extract_dn_from_text(text: str) -> Optional[str]:
    m = re.search(r"\bDN\s*0?(\d{2,3})\b", text or "", re.IGNORECASE)
    return f"DN{m.group(1)}" if m else None




def _extract_flow_from_html_text(text: str) -> Tuple[Optional[float], Optional[str]]:
    if not text:
        return None, None
    m = re.search(r"leistung[^\n\r]{0,80}?([0-9]+(?:[\.,][0-9]+)?)\s*l\s*/\s*s", text, re.IGNORECASE)
    if not m:
        m = re.search(r"([0-9]+(?:[\.,][0-9]+)?)\s*l\s*/\s*s", text, re.IGNORECASE)
    if not m:
        return None, None
    try:
        return float(m.group(1).replace(',', '.')), m.group(0)
    except Exception:
        return None, None


def _extract_height_from_text_blob(text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    if not text:
        return None, None, None
    m = re.search(r"(einbauhöhe|bauhöhe|installationshöhe)[^\n\r\.]{0,140}", text, re.IGNORECASE)
    if not m:
        return None, None, None
    start = max(0, m.start() - 20)
    end = min(len(text), m.end() + 260)
    snippet = text[start:end]
    vals: List[int] = []
    for mm in MM_RE.finditer(snippet):
        v = int(mm.group(1))
        if 20 <= v <= 300:
            vals.append(v)
    if not vals:
        return None, None, snippet
    return min(vals), max(vals), snippet


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
    if st != 200 or not html:
        return res

    full_text = _clean_text(BeautifulSoup(html, "lxml").get_text(" ", strip=True))
    pdf_links = _find_datasheet_pdf_links(html, final)

    # Height from HTML first.
    hmin, hmax, hsnip = _extract_height_from_text_blob(full_text)
    if hmin is not None and hmax is not None:
        res["height_adj_min_mm"] = hmin
        res["height_adj_max_mm"] = hmax
        if hsnip:
            res["evidence"].append(("Height", hsnip[:420], final))

    # Prefer PDF for flow/DN (and height fallback).
    for pdf_url in pdf_links[:5]:
        pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
        res["evidence"].append(("PDF status", pdf_status, pdf_url))
        if not pdf_text:
            continue

        if res.get("flow_rate_lps") is None:
            lps, raw_txt, unit, status = select_flow_rate(pdf_text)
            if lps is not None:
                res["flow_rate_lps"] = lps
                res["flow_rate_raw_text"] = raw_txt
                res["flow_rate_unit"] = unit
                res["flow_rate_status"] = status
                res["evidence"].append(("Flow rate", raw_txt or f"{lps} {unit or 'l/s'}", pdf_url))

        if res.get("outlet_dn") is None:
            dn = _extract_dn_from_text(pdf_text)
            if dn:
                res["outlet_dn"] = dn
                res["outlet_dn_default"] = dn
                res["outlet_dn_options_json"] = json.dumps([dn], ensure_ascii=False)
                res["evidence"].append(("Outlet DN", dn, pdf_url))

        if res.get("height_adj_min_mm") is None:
            phmin, phmax, phsnip = _extract_height_from_text_blob(_clean_text(pdf_text))
            if phmin is not None and phmax is not None:
                res["height_adj_min_mm"] = phmin
                res["height_adj_max_mm"] = phmax
                if phsnip:
                    res["evidence"].append(("Height", phsnip[:420], pdf_url))

        if res.get("flow_rate_lps") is not None and res.get("outlet_dn") is not None and res.get("height_adj_min_mm") is not None:
            break

    # If no PDFs present, fall back to HTML for flow/DN.
    if not pdf_links:
        flow_html, flow_raw = _extract_flow_from_html_text(full_text)
        if flow_html is not None:
            res["flow_rate_lps"] = flow_html
            res["flow_rate_raw_text"] = flow_raw
            res["flow_rate_unit"] = "l/s"
            res["flow_rate_status"] = "parsed_html"
            res["evidence"].append(("Flow rate", flow_raw or f"{flow_html} l/s", final))

        dn_html = _extract_dn_from_text(full_text)
        if dn_html:
            res["outlet_dn"] = dn_html
            res["outlet_dn_default"] = dn_html
            res["outlet_dn_options_json"] = json.dumps([dn_html], ensure_ascii=False)
            res["evidence"].append(("Outlet DN", dn_html, final))

    return res

