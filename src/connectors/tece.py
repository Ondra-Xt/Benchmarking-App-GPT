from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import gzip
import json
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from ..flowrate import select_flow_rate
from ..pdf_text import extract_pdf_text_from_url


BASE = "https://www.tece.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de,de-DE;q=0.9,en;q=0.8,cs;q=0.7",
    "Connection": "keep-alive",
}

KEYWORDS = ["drain", "shower-channel", "duschrinne", "tecedrainline", "tecedrainprofile"]
_LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE | re.DOTALL)


def _abs(href: str, base_url: str) -> str:
    return urljoin(base_url, href or "")


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


def _crawl_sitemaps(start_sitemaps: List[str], max_sitemaps: int = 250, max_pages: int = 200000) -> Tuple[List[str], List[Dict[str, Any]]]:
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


def _find_pdf_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    soup = BeautifulSoup(html.replace("\\/", "/"), "lxml")
    pdfs = []
    for a in soup.select("a[href*='.pdf']"):
        href = a.get("href") or ""
        if ".pdf" in href.lower():
            pdfs.append(_abs(href, base_url))
    return list(dict.fromkeys(pdfs))


def _extract_height_mm(text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    t = _clean_text(text)
    m = re.search(r"(installation height|einbauh(?:ö|oe)he|bauh(?:ö|oe)he|mindestbauh(?:ö|oe)he)\s*:?", t, re.IGNORECASE)
    if not m:
        return None, None, None
    seg = t[m.end(): min(len(t), m.end() + 220)]
    vals = []
    for vm in re.finditer(r"(\d{1,3})\s*mm", seg, re.IGNORECASE):
        v = int(vm.group(1))
        if 1 <= v <= 300:
            vals.append(v)
        if len(vals) >= 2:
            break
    if not vals:
        return None, None, None
    hmin = vals[0]
    hmax = vals[1] if len(vals) > 1 else vals[0]
    if hmax < hmin:
        hmin, hmax = hmax, hmin
    return hmin, hmax, t[max(0, m.start() - 20):min(len(t), m.end() + 220)]


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    _ = target_length_mm, tolerance_mm
    sitemaps = _robots_sitemaps(BASE)
    pages, debug = _crawl_sitemaps(sitemaps)

    out: List[Dict[str, Any]] = []
    for u in pages:
        ul = (u or "").lower()
        if not u.startswith("http"):
            continue
        if not any(k in ul for k in KEYWORDS):
            continue
        if any(k in ul for k in ["download", ".pdf", "service", "news"]):
            continue

        out.append({
            "manufacturer": "tece",
            "product_family": "Drain",
            "product_name": u.split("/")[-1].replace("-", " "),
            "product_url": u,
            "sources": u,
            "candidate_type": "drain",
            "complete_system": "yes",
        })

    debug.append({"site": "tece", "seed_url": BASE + "/sitemap.xml", "status_code": 200 if out else None, "final_url": BASE + "/sitemap.xml", "error": "" if out else "No candidates after keyword filters.", "candidates_found": len(out), "method": "final", "is_index": None})
    return out, debug


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

    page_text = ""
    pdf_links: List[str] = []
    if st == 200 and html:
        soup = BeautifulSoup(html, "lxml")
        page_text = soup.get_text(" ", strip=True) or ""
        pdf_links = _find_pdf_links(html, final)

    # PDF first
    for pdf_url in (pdf_links or [])[:3]:
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
                if raw_txt:
                    res["evidence"].append(("Flow rate", raw_txt, pdf_url))

        if res.get("outlet_dn") is None:
            m = re.search(r"\bDN\s*0?(\d{2,3})\b", pdf_text, re.IGNORECASE)
            if m:
                dn = f"DN{m.group(1)}"
                res["outlet_dn"] = dn
                res["outlet_dn_default"] = dn
                res["outlet_dn_options_json"] = json.dumps([dn], ensure_ascii=False)
                res["evidence"].append(("Outlet DN", dn, pdf_url))

        if res.get("height_adj_min_mm") is None or res.get("height_adj_max_mm") is None:
            hmin, hmax, hsnip = _extract_height_mm(pdf_text)
            if hmin is not None and hmax is not None:
                res["height_adj_min_mm"] = hmin
                res["height_adj_max_mm"] = hmax
                if hsnip:
                    res["evidence"].append(("Installation/Construction height (mm)", hsnip, pdf_url))

    # HTML fallback
    if page_text:
        if res.get("flow_rate_lps") is None:
            lps, raw_txt, unit, status = select_flow_rate(page_text)
            if lps is not None:
                res["flow_rate_lps"] = lps
                res["flow_rate_raw_text"] = raw_txt
                res["flow_rate_unit"] = unit
                res["flow_rate_status"] = status
                if raw_txt:
                    res["evidence"].append(("Flow rate", raw_txt, final))

        if res.get("outlet_dn") is None:
            m = re.search(r"\bDN\s*0?(\d{2,3})\b", page_text, re.IGNORECASE)
            if m:
                dn = f"DN{m.group(1)}"
                res["outlet_dn"] = dn
                res["outlet_dn_default"] = dn
                res["outlet_dn_options_json"] = json.dumps([dn], ensure_ascii=False)
                res["evidence"].append(("Outlet DN", dn, final))

        if res.get("height_adj_min_mm") is None or res.get("height_adj_max_mm") is None:
            hmin, hmax, hsnip = _extract_height_mm(page_text)
            if hmin is not None and hmax is not None:
                res["height_adj_min_mm"] = hmin
                res["height_adj_max_mm"] = hmax
                if hsnip:
                    res["evidence"].append(("Installation/Construction height (mm)", hsnip, final))

    if res.get("outlet_dn") is None:
        res["outlet_dn"] = "DN50"
        res["outlet_dn_default"] = "DN50"
        res["outlet_dn_options_json"] = json.dumps(["DN50"], ensure_ascii=False)
        res["evidence"].append(("Outlet DN", "DN50 (default)", src))

    return res
