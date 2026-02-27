from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
import re
from pathlib import Path
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

from ..flowrate import select_flow_rate
from ..pdf_text import extract_pdf_text_from_url

BASE = "https://www.hansgrohe.de"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de,de-DE;q=0.9,en;q=0.8,cs;q=0.7",
    "Connection": "keep-alive",
}


# --- helpers -------------------------------------------------------------

def _base_from_url(url: str) -> str:
    try:
        p = urlparse(url)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}"
    except Exception:
        pass
    return BASE

def _abs_url(href: str, base: str) -> str:
    return urljoin(base, href or "")

def _safe_get(url: str, timeout: int = 30) -> Tuple[Optional[int], str, str, str]:
    """Returns: (status_code, final_url, text, error)"""
    try:
        r = requests.get(url, timeout=timeout, headers=HEADERS, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"

def _extract_article_no(url: str) -> Optional[str]:
    m = re.search(r"(\d{8})(?:\D*$)", url)
    return m.group(1) if m else None

def _is_finish_set(url: str) -> bool:
    u = (url or "").lower()
    # DE/EN
    if "fertigset" in u or "finish-set" in u or "finishset" in u:
        return True
    # CZ
    if "vrchni-sada" in u or "vrchn%C3%AD-sada".lower() in u:
        return True
    return False


# --- sitemap discovery (sitemap-first, protože line pages jsou JS) --------

def _probe_sitemap_urls(base: str) -> List[str]:
    # nejčastější umístění sitemap na různých CMS
    b = base.rstrip("/")
    return [
        f"{b}/robots.txt",
        f"{b}/sitemap.xml",
        f"{b}/sitemap_index.xml",
        f"{b}/sitemapindex.xml",
        f"{b}/sitemap",
    ]

def _extract_sitemaps_from_robots(robots_txt: str) -> List[str]:
    out = []
    for line in (robots_txt or "").splitlines():
        line = line.strip()
        if not line.lower().startswith("sitemap:"):
            continue
        url = line.split(":", 1)[1].strip()
        if url:
            out.append(url)
    return out

def _fetch_xml(url: str, timeout: int = 30) -> Tuple[Optional[int], str, str]:
    status, final_url, text, err = _safe_get(url, timeout=timeout)
    if status != 200 or not text:
        return status, final_url, err or "empty"
    return status, final_url, text

def _parse_sitemap(xml_text: str) -> Tuple[List[str], List[str]]:
    """
    Returns (urls, nested_sitemaps)
    Supports <urlset> and <sitemapindex>.
    """
    urls: List[str] = []
    sitemaps: List[str] = []

    if not xml_text:
        return urls, sitemaps

    try:
        root = ET.fromstring(xml_text.encode("utf-8", errors="ignore"))
    except Exception:
        # někdy přijde HTML nebo jiný obsah
        return urls, sitemaps

    tag = root.tag.lower()
    if tag.endswith("sitemapindex"):
        for sm in root.findall(".//{*}sitemap/{*}loc"):
            if sm.text:
                sitemaps.append(sm.text.strip())
    elif tag.endswith("urlset"):
        for loc in root.findall(".//{*}url/{*}loc"):
            if loc.text:
                urls.append(loc.text.strip())

    return urls, sitemaps

def _collect_urls_from_sitemaps(
    base: str,
    contains_all: List[str],
    contains_any: List[str],
    max_urls: int = 2000,
    max_sitemaps: int = 50,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Robustní sběr URL ze sitemap:
      1) zkus robots.txt → sitemap links
      2) zkus pár běžných sitemap URL
      3) rekurzivně projdi sitemapindex
    """
    debug: List[Dict[str, Any]] = []
    candidates = _probe_sitemap_urls(base)

    sitemap_urls: List[str] = []
    tried = set()

    # 1) robots.txt
    st, final, txt, err = _safe_get(candidates[0])
    debug.append({"seed_url": candidates[0], "status_code": st, "final_url": final, "error": err, "method": "robots"})
    if st == 200 and txt:
        sitemap_urls.extend(_extract_sitemaps_from_robots(txt))

    # 2) fallback sitemap locations
    for u in candidates[1:]:
        if u in tried:
            continue
        tried.add(u)
        st2, final2, xml_or_err = _fetch_xml(u)
        debug.append({"seed_url": u, "status_code": st2, "final_url": final2, "error": "" if st2 == 200 else xml_or_err, "method": "probe"})
        if st2 == 200:
            sitemap_urls.append(final2)

    # dedup
    sitemap_urls = list(dict.fromkeys([s for s in sitemap_urls if s]))

    urls_out: List[str] = []
    to_visit = sitemap_urls[:]
    visited = set()

    while to_visit and len(visited) < max_sitemaps and len(urls_out) < max_urls:
        sm = to_visit.pop(0)
        if sm in visited:
            continue
        visited.add(sm)

        st3, final3, xml_text_or_err = _fetch_xml(sm)
        debug.append({"seed_url": sm, "status_code": st3, "final_url": final3, "error": "" if st3 == 200 else xml_text_or_err, "method": "sitemap"})
        if st3 != 200:
            continue

        urls, nested = _parse_sitemap(xml_text_or_err)
        # přidej nested sitemaps
        for n in nested:
            if n and n not in visited and n not in to_visit:
                to_visit.append(n)

        # filtruj urls
        for u in urls:
            lu = u.lower()
            if contains_all and not all(x.lower() in lu for x in contains_all):
                continue
            if contains_any and not any(x.lower() in lu for x in contains_any):
                continue
            urls_out.append(u)
            if len(urls_out) >= max_urls:
                break

    urls_out = list(dict.fromkeys(urls_out))
    return urls_out, debug


# --- parsers -------------------------------------------------------------

def _material_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None
    m = re.search(r"\b(1\.4404|1\.4301|316L|304|V4A|V2A)\b", text, re.IGNORECASE)
    if not m:
        return None, None
    token = m.group(1)
    tok = token.lower()
    if tok in ("1.4404", "316l", "v4a"):
        return token, "yes"
    if tok in ("1.4301", "304", "v2a"):
        return token, "no"
    return token, None

def _yes_if_found(pattern: str, text: str) -> Optional[str]:
    if not text:
        return None
    return "yes" if re.search(pattern, text, re.IGNORECASE) else None

def _parse_height_range_mm(text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    if not text:
        return None, None, None
    t = " ".join(text.split())
    for m in re.finditer(r"(\d{1,3})\s*[–-]\s*(\d{1,3})\s*mm", t, flags=re.IGNORECASE):
        a = int(m.group(1)); b = int(m.group(2))
        if 0 <= a <= 300 and 0 <= b <= 300 and b >= a:
            lo = max(0, m.start() - 80)
            hi = min(len(t), m.end() + 120)
            snip = t[lo:hi]
            snl = snip.lower()
            if any(k in snl for k in ["höhe", "height", "rahmen", "verstell", "höhenverstell", "install", "tile", "fliesen"]):
                return a, b, snip
    return None, None, None

def _parse_outlet_dn(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Chytá DN i v kontextu "Ablaufanschluss", "Abflussanschluss", "nominal diameter", atd.
    """
    if not text:
        return None, None
    t = " ".join(text.split())

    patterns = [
        r"\bDN\s*0?(\d{2})\b",
        r"\b(Ablauf|Abfluss)\w*\s*(?:anschluss|connection|Anschluss)\s*[:\-]?\s*DN\s*0?(\d{2})\b",
        r"\b(?:nominal|nominale)\s+(?:diameter|weite)\s*[:\-]?\s*DN\s*0?(\d{2})\b",
    ]
    for pat in patterns:
        m = re.search(pat, t, re.IGNORECASE)
        if not m:
            continue
        # DN může být v group(1) nebo group(2)
        dn_num = m.group(m.lastindex) if m.lastindex else None
        if not dn_num:
            continue
        dn = f"DN{dn_num}"
        lo = max(0, m.start() - 80)
        hi = min(len(t), m.end() + 160)
        return dn, t[lo:hi]

    return None, None


def _merge_flow(res: Dict[str, Any], lps, raw_txt, unit, status, src: str):
    if lps is None:
        return
    rank = {"ok": 3, "ok_no_en1253": 2, "unknown_no_context": 1, "unknown_no_units": 0, "rejected_out_of_range": -1}
    cur_status = res.get("flow_rate_status")
    cur_rank = rank.get(str(cur_status), -2)
    new_rank = rank.get(str(status), -2)

    if res.get("flow_rate_lps") is None or new_rank > cur_rank:
        res["flow_rate_lps"] = lps
        res["flow_rate_raw_text"] = raw_txt
        res["flow_rate_unit"] = unit
        res["flow_rate_status"] = status
        if raw_txt:
            res["evidence"].append(("Flow rate", raw_txt, src))


def _apply_text_extraction(res: Dict[str, Any], text: str, src: str):
    if not text:
        return

    # 1) Flow rate
    lps, raw_txt, unit, status = select_flow_rate(text)
    _merge_flow(res, lps, raw_txt, unit, status, src)

    # 2) Material
    detail, v4a = _material_from_text(text)
    if detail and res.get("material_detail") is None:
        res["material_detail"] = detail
        res["material_v4a"] = v4a
        res["evidence"].append(("Material", detail, src))

    # 3) EN 1253 (robustněji)
    if res.get("din_en_1253_cert") is None:
        yn = _yes_if_found(r"(?:DIN\s*)?EN\s*1253(?:\s*[-–]\s*\d+)?", text)
        if yn:
            res["din_en_1253_cert"] = yn
            res["evidence"].append(("DIN EN 1253", "EN 1253 nalezeno", src))

    # 4) DIN 18534
    if res.get("din_18534_compliance") is None:
        yn = _yes_if_found(r"(?:DIN\s*/\s*EN|DIN|EN)\s*18534", text)
        if yn:
            res["din_18534_compliance"] = yn
            res["evidence"].append(("DIN 18534", "DIN/EN 18534 nalezeno", src))

    # 5) Height adjustability range
    if res.get("height_adj_min_mm") is None or res.get("height_adj_max_mm") is None:
        a, b, snip = _parse_height_range_mm(text)
        if a is not None and b is not None:
            res["height_adj_min_mm"] = a
            res["height_adj_max_mm"] = b
            if snip:
                res["evidence"].append(("Height adjustability", snip, src))

    # 6) Outlet DN
    if res.get("outlet_dn") is None:
        dn, snip = _parse_outlet_dn(text)
        if dn:
            res["outlet_dn"] = dn
            if snip:
                res["evidence"].append(("Outlet DN", snip, src))

    # 7) Sealing fleece preassembled (heuristika)
    if res.get("sealing_fleece_preassembled") is None:
        tl = text.lower()
        if re.search(r"dicht(man|vl)sch", tl) and ("vormont" in tl or "pre-assembl" in tl):
            res["sealing_fleece_preassembled"] = "yes"
            res["evidence"].append(("Sealing fleece", "dicht… + vormont…", src))


def _pick_best_pdf_link(candidates: List[Tuple[str, str]], article_no: Optional[str]) -> Optional[str]:
    if not candidates:
        return None

    def score(href: str, txt: str) -> int:
        h = (href or "").lower()
        t = (txt or "").lower()
        s = 0
        if h.endswith(".pdf"):
            s += 2
        for k in ["product_specification", "produktdatenblatt", "product specification", "datasheet", "technical data"]:
            if k in h or k in t:
                s += 6
        if article_no and article_no in h:
            s += 5
        if "pdf" in h or "pdf" in t:
            s += 1
        return s

    ranked = sorted(candidates, key=lambda x: score(x[0], x[1]), reverse=True)
    best_href = ranked[0][0]
    return best_href if best_href else None

def _find_pdf_url_in_html(html: str, base: str, article_no: Optional[str]) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    pdf_links: List[Tuple[str, str]] = []

    for a in soup.select("a[href$='.pdf'], a[href*='.pdf']"):
        href = a.get("href", "") or ""
        txt = a.get_text(" ", strip=True) or ""
        if ".pdf" in href.lower():
            pdf_links.append((_abs_url(href, base), txt))

    return _pick_best_pdf_link(pdf_links, article_no)


# --- public API ----------------------------------------------------------

def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    """
    Hansgrohe discovery: sitemap-first (line pages jsou často JS).
    Vrací (found, debug)
    """
    found: List[Dict[str, Any]] = []

    urls, debug = _collect_urls_from_sitemaps(
        base=BASE,
        contains_all=["articledetail-"],
        contains_any=["raindrain"],
        max_urls=2500,
        max_sitemaps=80,
    )

    # fallback: když sitemap nevrátí nic, aspoň vrať prázdno s debugem
    for u in urls:
        title = u.split("/")[-1].replace("-", " ").strip()
        found.append({
            "manufacturer": "hansgrohe",
            "product_family": "RainDrain",
            "product_name": title,
            "product_url": u,
            "length_mode": "unknown",
            "available_lengths_mm": "",
            "selected_length_mm": target_length_mm,
            "length_delta_mm": None,
            "complete_system": "unknown",
            "candidate_type": "product_detail",
        })

    # doplň debug metadatama
    for d in debug:
        d.setdefault("site", "hansgrohe")
        d.setdefault("candidates_found", len(found))

    return found, debug


def get_bom_options(product_url: str, extracted_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Pro finish sety (fertigset / vrchní sada) vrací BOM volby uBox:
      - DN40 (flat)
      - DN50 (standard)  -> default
    """
    if not _is_finish_set(product_url):
        return []

    # Odkazy (uživatel je poslal) – držíme jako evidence / reference
    ubox_flat = "https://www.hansgrohe.cz/articledetail-ubox-universal-zakladni-teleso-pro-sprchove-zlaby-pro-plochou-instalaci-01000180"
    ubox_std = "https://www.hansgrohe.com/articledetail-ubox-universal-base-set-for-finish-sets-for-standard-installation-01001180"

    base_id = _extract_article_no(product_url) or "finishset"
    return [
        {
            "option_id": f"{base_id}:UBOX-FLAT-DN40",
            "option_name": "uBox universal – plochá instalace (DN40)",
            "outlet_dn": "DN40",
            "related_product_url": ubox_flat,
            "is_default": False,
        },
        {
            "option_id": f"{base_id}:UBOX-STD-DN50",
            "option_name": "uBox universal – standardní instalace (DN50)",
            "outlet_dn": "DN50",
            "related_product_url": ubox_std,
            "is_default": True,
        },
    ]


def extract_parameters(product_url: str) -> Dict[str, Any]:
    """
    PDF-first strategie:
      - když je product_url PDF (URL) => extrakce z PDF
      - jinak: stáhni HTML, zkus najít PDF link, extrahuj z PDF
      - fallback: extrahuj z HTML textu
    + finish set: DN typicky není na vrchní sadě → default DN50 + BOM volby
    """
    res: Dict[str, Any] = {
        "flow_rate_lps": None,
        "flow_rate_raw_text": None,
        "flow_rate_unit": None,
        "flow_rate_status": None,

        "material_detail": None,
        "material_v4a": None,

        "din_en_1253_cert": None,
        "din_18534_compliance": None,

        "height_adj_min_mm": None,
        "height_adj_max_mm": None,

        "outlet_selectable": None,
        "outlet_two_variants": None,
        "outlet_direction": None,
        "outlet_dn": None,

        "sealing_fleece_preassembled": None,
        "colours_count": None,

        "evidence": [],
    }

    src = (product_url or "").strip()

    # 1) Přímé PDF URL
    if src.lower().startswith("http") and src.lower().endswith(".pdf"):
        pdf_text, pdf_status = extract_pdf_text_from_url(src, headers=HEADERS)
        res["evidence"].append(("PDF status", f"{pdf_status}", src))
        if pdf_text:
            _apply_text_extraction(res, pdf_text, src)

    else:
        # 2) HTML -> zkus najít PDF + fallback HTML text
        base = _base_from_url(src)
        article_no = _extract_article_no(src)

        status_code, final_url, html, err = _safe_get(src)
        res["evidence"].append(("HTML fetch", f"status={status_code} err={err}".strip(), final_url))

        pdf_url = None
        if status_code == 200 and html:
            pdf_url = _find_pdf_url_in_html(html, _base_from_url(final_url), article_no)

        # 2a) PDF-first
        if pdf_url:
            pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
            res["evidence"].append(("PDF url", pdf_url, pdf_url))
            res["evidence"].append(("PDF status", pdf_status, pdf_url))
            if pdf_text:
                _apply_text_extraction(res, pdf_text, pdf_url)

        # 2b) fallback z HTML
        if status_code == 200 and html:
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True)
            if text:
                _apply_text_extraction(res, text, final_url)

    # --- Finish set logic: DN je BOM volba (DN40/DN50); default DN50 ---
    if _is_finish_set(src):
        res["outlet_selectable"] = "yes"
        res["outlet_two_variants"] = "yes"

        if res.get("outlet_dn") is None:
            # default DN50 (uživatel chce)
            res["outlet_dn"] = "DN50"
            res["evidence"].append(("Outlet DN (default)", "Finish set → DN dle zvoleného uBoxu; default DN50", src))

    return res