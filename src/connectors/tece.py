from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
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
    "Accept-Language": "de,de-DE;q=0.9,en;q=0.8,cs;q=0.7",
    "Connection": "keep-alive",
}

PR_PATH_RE = re.compile(r"/PR/(\d+)/index\.xhtml(?:;jsessionid=[^/?#]+)?$", re.IGNORECASE)
PR_LINK_RE = re.compile(r"https?://[^\s\"'>]*/PR/\d+/index\.xhtml(?:;jsessionid=[^\s\"'>]+)?", re.IGNORECASE)
LENGTH_RE = re.compile(r"(\d{3,4})\s*mm", re.IGNORECASE)
MM_RE = re.compile(r"(\d{1,3})\s*mm", re.IGNORECASE)

SEED_URLS = [
    "https://productdaten.tece.de/web/tece/de_DE/tece/PR/601202/index.xhtml",
    "https://productdaten.tece.de/web/tece/de_DE/tece/PR/601201/index.xhtml",
    "https://productdaten.tece.de/web/tece/de_DE/tece/PR/601200/index.xhtml",
    "https://productdaten.tece.de/web/tece/de_DE/tece/PR/671200/index.xhtml",
]


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
def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _is_allowed_tece_url(url: str) -> bool:
    url = _canonicalize_url(url)
    try:
        p = urlparse(url or "")
    except Exception:
        return False
    if p.netloc.lower() != "productdaten.tece.de":
        return False
    pl = (p.path or "").lower()
    if "/web/tece/de_de/tece/" not in pl:
        return False
    if any(x in pl for x in ["academy", "magazine", "certificates", "instructions"]):
        return False
    return True


def _is_pr_product_page(url: str) -> bool:
    url = _canonicalize_url(url)
    try:
        p = urlparse(url or "")
    except Exception:
        return False
    return _is_allowed_tece_url(url) and (PR_PATH_RE.search(p.path + (";" + p.params if p.params else "")) is not None)


def _extract_pr_number(url: str) -> Optional[str]:
    url = _canonicalize_url(url)
    try:
        p = urlparse(url or "")
    except Exception:
        return None
    m = PR_PATH_RE.search(p.path + (";" + p.params if p.params else ""))
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


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    min_len = max(0, int(target_length_mm) - int(tolerance_mm))
    max_len = int(target_length_mm) + int(tolerance_mm)

    debug: List[Dict[str, Any]] = []
    queue = [_canonicalize_url(u) for u in SEED_URLS]
    seen = set()
    found_pr_urls = set()

    while queue and len(seen) < 400:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)

        st, final, html, err = _safe_get_text(url, timeout=30)
        final_c = _canonicalize_url(final)
        discovered_links = 0
        accepted_pr_links = 0

        if st == 200 and html:
            if _is_pr_product_page(final_c):
                found_pr_urls.add(final_c)
                accepted_pr_links += 1

            links = _extract_product_links(html, final_c)
            discovered_links = len(links)
            for lk in links:
                if _is_pr_product_page(lk):
                    if lk not in found_pr_urls:
                        found_pr_urls.add(lk)
                    accepted_pr_links += 1
                elif lk not in seen and lk not in queue and len(queue) < 700:
                    queue.append(lk)

        debug.append({
            "site": "tece",
            "seed_url": url,
            "status_code": st,
            "final_url": final_c,
            "error": err,
            "candidates_found": discovered_links,
            "candidates_accepted": accepted_pr_links,
            "method": "seed_bfs",
            "is_index": None,
        })

    out: List[Dict[str, Any]] = []
    filtered_out_no_length = 0
    filtered_out_len_window = 0

    for u in sorted(found_pr_urls):
        if len(out) >= 200:
            break
        if not _is_pr_product_page(u):
            continue

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
        length_mm = _extract_length_from_text(name)
        if length_mm is None:
            filtered_out_no_length += 1
            continue
        if not (min_len <= length_mm <= max_len):
            filtered_out_len_window += 1
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
            "seed_url": u,
            "status_code": st,
            "final_url": final_c,
            "error": err,
            "candidates_found": 1,
            "candidates_accepted": 1,
            "method": "product_filter",
            "is_index": None,
        })

    debug.append({
        "site": "tece",
        "seed_url": "summary",
        "status_code": 200,
        "final_url": "produktdaten.tece.de",
        "error": "",
        "candidates_found": len(found_pr_urls),
        "candidates_accepted": len(out),
        "filtered_no_length": filtered_out_no_length,
        "filtered_out_of_window": filtered_out_len_window,
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
