"""TECE connector (full MVP implementation)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import json
import gzip
import re
from urllib.parse import unquote, urljoin, urlparse

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
LENGTH_RE = re.compile(r"(\d{3,4})\s*mm", re.IGNORECASE)
MM_RE = re.compile(r"(\d{1,3})\s*mm", re.IGNORECASE)
DE_TECE_PATH_RE = re.compile(r"^/web/[^/]+/de_DE/tece/.*", re.IGNORECASE)
BASE = "https://produktdaten.tece.de"
TECE_COM_BASE = "https://www.tece.com"
TECE_COM_INCLUDE = ("entwaesserungstechnik", "duschrinne", "drainline", "drainprofile")
TECE_COM_EXCLUDE = ("academy", "service", "download", "presse", "magazin", "zubehoer", "datenblatt", "montage", "anleitung", ".pdf")
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
    url = _canonicalize_url(url)
    try:
        p = urlparse(url or "")
    except Exception:
        return False
    if p.netloc.lower() != "produktdaten.tece.de":
        return False
    path_decoded = unquote(p.path or "")
    if DE_TECE_PATH_RE.search(path_decoded) is None:
        return False
    if any(x in path_decoded.lower() for x in ["academy", "magazine", "certificates", "instructions"]):
        return False
    return True


def _is_pr_product_page(url: str) -> bool:
    url = _canonicalize_url(url)
    try:
        p = urlparse(url or "")
    except Exception:
        return False
    path_decoded = unquote(p.path or "")
    return _is_allowed_tece_url(url) and (PR_PATH_RE.search(path_decoded) is not None)


def _extract_pr_number(url: str) -> Optional[str]:
    url = _canonicalize_url(url)
    try:
        p = urlparse(url or "")
    except Exception:
        return None
    m = PR_PATH_RE.search(unquote(p.path or ""))
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

    soup = BeautifulSoup(html.replace("\\/", "/"), "lxml")
    out: List[str] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        target = _canonicalize_url(urljoin(base_url, href))
        if not _is_allowed_tece_url(target):
            continue

        # Only keep PR product pages, but allow variant pages that can lead to PR pages.
        txt = _clean_text(a.get_text(" ", strip=True)).lower()
        if _is_pr_product_page(target) or ("varianten" in txt) or ("alle produkte" in txt):
            out.append(target)

    for m in PR_LINK_RE.finditer(html.replace("\\/", "/")):
        target = _canonicalize_url(m.group(0))
        if _is_allowed_tece_url(target):
            out.append(target)

    return list(dict.fromkeys(out))


def _extract_length_from_url(url: str) -> Optional[int]:
    path_decoded = unquote(urlparse(url or "").path or "")
    vals = []
    for m in LENGTH_RE.finditer(path_decoded):
        try:
            v = int(m.group(1))
            if 300 <= v <= 3000:
                vals.append(v)
        except Exception:
            continue
    return vals[0] if vals else None


def _extract_length_from_text(name: str) -> Optional[int]:
    vals = []
    for m in LENGTH_RE.finditer(name or ""):
        try:
            v = int(m.group(1))
            if 300 <= v <= 3000:
                vals.append(v)
        except Exception:
            continue
    return vals[0] if vals else None


def _is_allowed_tececom_url(url: str) -> bool:
    try:
        p = urlparse(url or "")
    except Exception:
        return False
    if p.netloc.lower() != "www.tece.com":
        return False
    path = unquote(p.path or "").lower()
    if not path.startswith("/de/"):
        return False
    if not any(k in path for k in TECE_COM_INCLUDE):
        return False
    if any(k in path for k in TECE_COM_EXCLUDE):
        return False
    return True


def _discover_from_tececom(max_pages: int = 80) -> Tuple[List[str], int, int, int]:
    start_sitemaps = _robots_sitemaps(TECE_COM_BASE)
    all_urls, _ = _crawl_sitemaps(start_sitemaps, max_sitemaps=200, max_pages=120000)

    considered = 0
    pages_fetched = 0
    pr_links_found = 0
    out: List[str] = []

    tece_pages: List[str] = []
    for u in all_urls:
        cu = _canonicalize_url(u)
        if _is_allowed_tececom_url(cu):
            considered += 1
            tece_pages.append(cu)

    for page_url in tece_pages[:max_pages]:
        st, final, html, _ = _safe_get_text(page_url, timeout=25)
        if st != 200 or not html:
            continue
        pages_fetched += 1

        final_c = _canonicalize_url(final)
        soup = BeautifulSoup(html.replace('\/', '/'), 'lxml')
        for a in soup.select('a[href]'):
            href = (a.get('href') or '').strip()
            if not href:
                continue
            if '/PR/' not in href or 'index.xhtml' not in href:
                continue
            target = _canonicalize_url(urljoin(final_c, href))
            if _is_pr_product_page(target):
                pr_links_found += 1
                out.append(target)

        for m in PR_LINK_RE.finditer(html.replace('\/', '/')):
            target = _canonicalize_url(m.group(0))
            if _is_pr_product_page(target):
                pr_links_found += 1
                out.append(target)

    return list(dict.fromkeys(out)), considered, pages_fetched, pr_links_found


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    min_len = max(0, int(target_length_mm) - int(tolerance_mm))
    max_len = int(target_length_mm) + int(tolerance_mm)

    start_sitemaps = _robots_sitemaps(BASE)
    all_urls, debug = _crawl_sitemaps(start_sitemaps)

    total = len(all_urls)
    host_filtered_urls = [_canonicalize_url(u) for u in all_urls if _is_allowed_tece_url(_canonicalize_url(u))]
    after_host_filter = len(set(host_filtered_urls))

    pr_urls = [_canonicalize_url(u) for u in host_filtered_urls if _is_pr_product_page(_canonicalize_url(u))]
    pr_urls = list(dict.fromkeys(pr_urls))
    after_pr_filter = len(pr_urls)

    sitemap_status_ok = any((d.get("method") == "sitemap" and d.get("status_code") == 200) for d in debug)
    sitemap_status = "ok" if sitemap_status_ok else "fail"
    fallback_used = False
    total_tececom_pages_considered = 0
    pages_fetched = 0
    pr_links_found = 0

    if (not sitemap_status_ok) or (not pr_urls):
        fallback_used = True
        fallback_pr_urls, total_tececom_pages_considered, pages_fetched, pr_links_found = _discover_from_tececom(max_pages=80)
        if fallback_pr_urls:
            pr_urls = list(dict.fromkeys(pr_urls + fallback_pr_urls))
            after_pr_filter = len(pr_urls)

    out: List[Dict[str, Any]] = []
    filtered_no_length = 0
    filtered_out_of_window = 0

    for u in pr_urls:
        if len(out) >= 300:
            break

        st, final, html, err = _safe_get_text(u, timeout=25)
        final_c = _canonicalize_url(final)
        if st != 200 or not html or not _is_pr_product_page(final_c):
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

        name = _extract_title_text(html)
        length_mm = _extract_length_from_text(unquote(final_c)) or _extract_length_from_text(name)
        if length_mm is None:
            filtered_no_length += 1
            continue
        if not (min_len <= length_mm <= max_len):
            filtered_out_of_window += 1
            continue

        pr_no = _extract_pr_number(final_c)
        if not pr_no:
            continue

        out.append({
            "manufacturer": "tece",
            "product_id": f"tece-{pr_no}",
            "product_family": "TECEdrain",
            "product_name": name,
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
        "final_url": BASE,
        "error": "",
        "total": total,
        "after_host_filter": after_host_filter,
        "after_pr_filter": after_pr_filter,
        "sitemap_status": sitemap_status,
        "total_urls_from_sitemaps": total,
        "total_tececom_pages_considered": total_tececom_pages_considered,
        "pages_fetched": pages_fetched,
        "pr_links_found": pr_links_found,
        "pr_urls_after_filters": after_pr_filter,
        "accepted_candidates": len(out),
        "fallback_used": "yes" if fallback_used else "no",
        "after_length_filter": after_pr_filter - filtered_no_length,
        "final_count": len(out),
        "candidates_found": len(pr_urls),
        "candidates_accepted": len(out),
        "filtered_no_length": filtered_no_length,
        "filtered_out_of_window": filtered_out_of_window,
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

    # height from Produktdaten HTML
    hmin, hmax, hsnip = _extract_height_from_product_html(html)
    if hmin is not None and hmax is not None:
        res["height_adj_min_mm"] = hmin
        res["height_adj_max_mm"] = hmax
        if hsnip:
            res["evidence"].append(("Installation/Construction height (mm)", hsnip, final))

    # flow/DN from datasheet PDF links on PR page
    pdf_links = _find_datasheet_pdf_links(html, final)
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
                if raw_txt:
                    res["evidence"].append(("Flow rate", raw_txt, pdf_url))

        if res.get("outlet_dn") is None:
            dn = _extract_dn_from_text(pdf_text)
            if dn:
                res["outlet_dn"] = dn
                res["outlet_dn_default"] = dn
                res["outlet_dn_options_json"] = json.dumps([dn], ensure_ascii=False)
                res["evidence"].append(("Outlet DN", dn, pdf_url))

        if res.get("flow_rate_lps") is not None and res.get("outlet_dn") is not None:
            break

    return res
