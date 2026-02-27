from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
import re
from urllib.parse import urljoin, quote
from functools import lru_cache

import requests
from bs4 import BeautifulSoup

from ..flowrate import select_flow_rate
from ..pdf_text import extract_pdf_text_from_url
from ..sitemap_utils import fetch_urls_from_sitemaps

BASE = "https://www.dallmer.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en,en-US;q=0.9,de;q=0.8,cs;q=0.7",
    "Connection": "keep-alive",
}

# veřejná brožura – fallback (primárně CeraLine, ale pomůže i obecně)
BROCHURE_PDFS = [
    # CeraLine (běžně veřejné a stabilní)
    "https://www.dallmer.com/default-wAssets/docs/int/broschueren/neuheiten/Shower-Channel-CeraLine.pdf",
]

# věci, které nechceme v registry (krytky, rošty, rámy, sifony, pasti, náhradní díly…)
EXCLUDE_MARKERS = (
    "cover-plate", "cover plate",
    "grate", "rost", "abdeckung", "cover",
    "frame", "rahmen",
    "spare", "ersatz",
    "trap", "siphon", "siphon", "geruchsverschluss",
    "washing machine", "dishwasher",
    "accessory", "zubehoer", "zubehör",
)

# naopak indikace “žlab / shower channel”
INCLUDE_MARKERS = (
    "shower-channel", "shower channel",
    "duschrinne",
    "caniveau",
)

PDF_HINTS = (
    "specification", "technical", "datenblatt", "product", "produkt",
    "installation", "montage", "instructions", "manual",
)

# ---------------------------------------------------------------------

def _abs_url(href: str) -> str:
    return urljoin(BASE, href or "")

def _safe_get_html(url: str, timeout: int = 30) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, timeout=timeout, headers=HEADERS, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"

@lru_cache(maxsize=2048)
def _head_ok(url: str, timeout: int = 12) -> bool:
    try:
        r = requests.head(url, timeout=timeout, headers=HEADERS, allow_redirects=True)
        return 200 <= int(r.status_code) < 400
    except Exception:
        return False

def _is_relevant_drain(url_or_title: str) -> bool:
    t = (url_or_title or "").lower()
    if any(x in t for x in EXCLUDE_MARKERS):
        return False
    return any(x in t for x in INCLUDE_MARKERS)

def _extract_links_from_html(html: str) -> List[str]:
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")

    urls: List[str] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = _abs_url(href)
        if "/produkte/" in full.lower():
            urls.append(full)

    # fallback: regex z HTML
    for m in re.findall(r'/(?:en|de|int)/produkte/[^\s"\'<>]+', html, flags=re.IGNORECASE):
        urls.append(_abs_url(m))

    out, seen = [], set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def _parse_length_mm(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.lower()
    m = re.search(r"(\d{3,4})\s*mm", t)
    if m:
        v = int(m.group(1))
        if 300 <= v <= 2000:
            return v
    # často ve slugu: ...-1200-mm...
    m = re.search(r"-(\d{3,4})-mm", t)
    if m:
        v = int(m.group(1))
        if 300 <= v <= 2000:
            return v
    return None

def _parse_dn(text: str) -> Optional[str]:
    """
    Robustní DN parsing i z URL typu ...-dn-50.php nebo "DN 50"
    """
    if not text:
        return None
    t = (text or "").lower()
    m = re.search(r"dn[^0-9]{0,6}0?(\d{2})", t)
    if m:
        return f"DN{m.group(1)}"
    return None

def _swap_dn_in_url(url: str, dn_from: str, dn_to: str) -> str:
    """
    Přepínač v URL:
      -dn-50-  -> -dn-40-
      -dn-50.php -> -dn-40.php
    """
    u = url
    u = re.sub(rf"dn[-_ ]?{re.escape(dn_from)}", f"dn-{dn_to}", u, flags=re.IGNORECASE)
    return u

def _find_pdf_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href*='.pdf']"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        links.append(full)
    # dedupe
    out, seen = [], set()
    for u in links:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def _score_pdf(url: str) -> int:
    u = (url or "").lower()
    s = 0
    if u.endswith(".pdf"):
        s += 2
    if any(h in u for h in PDF_HINTS):
        s += 6
    if "ceraline" in u or "shower-channel" in u or "duschrinne" in u:
        s += 3
    return s

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

def _norm_spaces(s: str) -> str:
    if not s:
        return ""
    # NBSP, tenké mezery apod.
    s = s.replace("\u00a0", " ").replace("\u202f", " ").replace("\u2009", " ")
    s = " ".join(s.split())
    return s

def _select_flow_rate_dallmer(text: str) -> Tuple[Optional[float], Optional[str], Optional[str], str]:
    """
    Dallmer-specific flow parser:
    - hledá 'Ablaufleistung', 'discharge capacity', 'Ablauf' v okolí a čísla s l/min nebo l/s
    - vrátí (lps, snippet, unit, status)
    """
    if not text:
        return None, None, None, "unknown_no_context"

    t = _norm_spaces(text)
    tl = t.lower()

    # 1) cílený kontextový regex
    ctx_patterns = [
        r"(ablaufleistung|discharge capacity|ablauf)\s*[:\-]?\s*([0-9]+(?:[.,][0-9]+)?)\s*(l\s*/\s*min|l/min|l\s*min|l\s*/\s*s|l/s)",
        r"([0-9]+(?:[.,][0-9]+)?)\s*(l\s*/\s*min|l/min|l\s*min)\s*(ablaufleistung|discharge capacity|ablauf)",
    ]
    for pat in ctx_patterns:
        m = re.search(pat, tl, flags=re.IGNORECASE)
        if m:
            nums = m.group(2) if len(m.groups()) >= 2 else m.group(1)
            unit = m.group(3) if len(m.groups()) >= 3 else m.group(2)
            val = float(nums.replace(",", "."))
            unit = unit.replace(" ", "")
            if "l/s" in unit:
                lps = val
                u = "l/s"
            else:
                lps = val / 60.0
                u = "l/min"
            lo = max(0, m.start() - 80)
            hi = min(len(t), m.end() + 120)
            return lps, t[lo:hi], u, "ok_no_en1253"

    # 2) obecný lov na l/min – vezmeme MAX hodnotu (typicky udávaná kapacita)
    vals = []
    for m in re.finditer(r"([0-9]+(?:[.,][0-9]+)?)\s*l\s*/\s*min", tl, flags=re.IGNORECASE):
        val = float(m.group(1).replace(",", "."))
        vals.append((val, m.start(), m.end()))
    if vals:
        val, a, b = max(vals, key=lambda x: x[0])
        lo = max(0, a - 80)
        hi = min(len(t), b + 120)
        return (val / 60.0), t[lo:hi], "l/min", "ok_no_en1253"

    # 3) obecný lov na l/s – vezmeme MAX
    vals = []
    for m in re.finditer(r"([0-9]+(?:[.,][0-9]+)?)\s*l\s*/\s*s", tl, flags=re.IGNORECASE):
        val = float(m.group(1).replace(",", "."))
        vals.append((val, m.start(), m.end()))
    if vals:
        val, a, b = max(vals, key=lambda x: x[0])
        lo = max(0, a - 80)
        hi = min(len(t), b + 120)
        return val, t[lo:hi], "l/s", "ok_no_en1253"

    return None, None, None, "unknown_no_units"

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

    # 1) nejdřív společný parser (pokud něco najde, bereme)
    lps, raw_txt, unit, status = select_flow_rate(text)
    _merge_flow(res, lps, raw_txt, unit, status, src)

    # 2) Dallmer-specific fallback (když společný nic)
    if res.get("flow_rate_lps") is None:
        lps2, raw2, unit2, status2 = _select_flow_rate_dallmer(text)
        _merge_flow(res, lps2, raw2, unit2, status2, src)

    # 3) Material
    detail, v4a = _material_from_text(text)
    if detail and res.get("material_detail") is None:
        res["material_detail"] = detail
        res["material_v4a"] = v4a
        res["evidence"].append(("Material", detail, src))

    # 4) EN 1253
    if res.get("din_en_1253_cert") is None:
        yn = _yes_if_found(r"(DIN\s*)?EN\s*1253", text)
        if yn:
            res["din_en_1253_cert"] = yn
            res["evidence"].append(("DIN EN 1253", "EN 1253 mentioned", src))

# ---------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------

def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    """
    Discovery pro všechny žlaby:
    - hledáme DN50 i DN40
    - filtrujeme na shower-channel / duschrinne / caniveau
    - vyhazujeme pasti/sifony/krytky
    """
    debug: List[Dict[str, Any]] = []
    found: List[Dict[str, Any]] = []
    seen: set[str] = set()

    search_terms = [
        "shower-channel dn 50",
        "shower-channel dn 40",
        "duschrinne dn 50",
        "duschrinne dn 40",
        "caniveau dn 50",
        "caniveau dn 40",
    ]

    for term in search_terms:
        url = f"{BASE}/en/search/?searchTerm={quote(term)}"
        status, final_url, html, err = _safe_get_html(url)
        added = 0

        if status == 200 and html:
            links = _extract_links_from_html(html)
            before = len(found)

            for u in links:
                if u in seen:
                    continue
                title = u.split("/")[-1].replace("-", " ").replace("_", " ").strip()

                if not _is_relevant_drain(u) and not _is_relevant_drain(title):
                    continue

                seen.add(u)

                length = _parse_length_mm(u) or _parse_length_mm(title)
                if length is not None:
                    delta = length - target_length_mm
                    keep = abs(delta) <= tolerance_mm
                    sel = length
                    mode = "fixed"
                else:
                    delta = None
                    keep = True
                    sel = target_length_mm
                    mode = "unknown"

                if not keep:
                    continue

                found.append({
                    "manufacturer": "dallmer",
                    "product_family": "Drain",
                    "product": title,
                    "sources": u,
                    "length_mode": mode,
                    "available_lengths_mm": "" if length is None else str(length),
                    "selected_length_mm": sel,
                    "length_delta_mm": delta,
                    "complete_system": "unknown",
                    "candidate_type": "product_detail",
                })

            added = len(found) - before

        debug.append({
            "site": "dallmer",
            "seed_url": url,
            "status_code": status,
            "final_url": final_url,
            "error": err,
            "candidates_found": added,
            "method": "search",
        })

    # sitemap fallback: jen pokud search nic
    if len(found) == 0:
        try:
            urls = fetch_urls_from_sitemaps(
                base_url=BASE,
                headers=HEADERS,
                contains_all=["/produkte/"],
                contains_any=["shower-channel", "duschrinne", "caniveau"],
                max_urls=5000,
                max_sitemaps=200,
            )
            before = len(found)
            for u in urls:
                if u in seen:
                    continue
                title = u.split("/")[-1].replace("-", " ").replace("_", " ").strip()
                if not _is_relevant_drain(u) and not _is_relevant_drain(title):
                    continue
                seen.add(u)
                found.append({
                    "manufacturer": "dallmer",
                    "product_family": "Drain",
                    "product": title,
                    "sources": u,
                    "length_mode": "unknown",
                    "available_lengths_mm": "",
                    "selected_length_mm": target_length_mm,
                    "length_delta_mm": None,
                    "complete_system": "unknown",
                    "candidate_type": "product_detail",
                })
            debug.append({
                "site": "dallmer",
                "seed_url": BASE + "/sitemap",
                "status_code": 200 if urls else None,
                "final_url": BASE + "/sitemap",
                "error": "" if urls else "No matching URLs found in sitemap",
                "candidates_found": len(found) - before,
                "method": "sitemap",
            })
        except Exception as e:
            debug.append({
                "site": "dallmer",
                "seed_url": BASE + "/sitemap",
                "status_code": None,
                "final_url": BASE + "/sitemap",
                "error": str(e),
                "candidates_found": 0,
                "method": "sitemap",
            })

    return found, debug

def get_bom_options(product: Any) -> List[Dict[str, Any]]:
    """
    BOM options: DN50 default + DN40 pokud existuje (a případně jiné DN, pokud by URL obsahovalo jinou DN).
    Kompatibilní volání: pipeline může poslat URL string nebo dict/row.
    """
    if isinstance(product, str):
        url = product
    elif isinstance(product, dict):
        url = product.get("product_url") or product.get("sources") or ""
    else:
        url = str(product)

    url = (url or "").strip()
    ul = url.lower()

    opts: List[Dict[str, Any]] = []

    # zjisti “aktuální” DN z URL
    dn = _parse_dn(ul)

    # default: DN50
    url_dn50 = url
    url_dn40 = None

    if dn == "DN40":
        url_dn50 = _swap_dn_in_url(url, "40", "50")
        url_dn40 = url
    elif dn == "DN50":
        url_dn50 = url
        url_dn40 = _swap_dn_in_url(url, "50", "40")
    else:
        # žádná DN v URL – necháme DN50 jako default (bez variant)
        url_dn50 = url

    # DN50 option vždy
    opts.append({
        "option_id": "DN50",
        "option_name": "Outlet DN50",
        "outlet_dn": "DN50",
        "related_product_url": url_dn50,
        "is_default": True,
        "note": "Default for Dallmer drains",
    })

    # DN40 option jen pokud URL existuje
    if url_dn40 and url_dn40 != url_dn50 and _head_ok(url_dn40):
        opts.append({
            "option_id": "DN40",
            "option_name": "Outlet DN40",
            "outlet_dn": "DN40",
            "related_product_url": url_dn40,
            "is_default": False,
            "note": "Alternative outlet DN40 (verified by HTTP HEAD)",
        })

    return opts

def extract_parameters(product_url: str) -> Dict[str, Any]:
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

        "sales_price_eur": None,
        "price_source_url": None,

        "evidence": [],
    }

    src = (product_url or "").strip()
    ul = src.lower()

    # DN z URL (a default DN50 když není)
    dn = _parse_dn(ul)
    if dn:
        res["outlet_dn"] = dn
        res["evidence"].append(("Outlet DN", f"{dn} (from URL)", src))
    else:
        res["outlet_dn"] = "DN50"
        res["evidence"].append(("Outlet DN", "DN50 (default)", src))

    # zjisti, zda existuje i alternativa DN40/DN50 => outlet_selectable / outlet_two_variants
    # (jen pokud je to žlab)
    if _is_relevant_drain(ul):
        url_dn40 = None
        url_dn50 = None
        if res["outlet_dn"] == "DN50":
            url_dn50 = src
            url_dn40 = _swap_dn_in_url(src, "50", "40") if "dn" in ul else None
        elif res["outlet_dn"] == "DN40":
            url_dn40 = src
            url_dn50 = _swap_dn_in_url(src, "40", "50")
        else:
            url_dn50 = src

        has_40 = bool(url_dn40 and url_dn40 != url_dn50 and _head_ok(url_dn40))
        has_50 = bool(url_dn50 and _head_ok(url_dn50))

        if has_40 and has_50:
            res["outlet_selectable"] = "yes"
            res["outlet_two_variants"] = "yes"
            res["evidence"].append(("Outlet variants", "DN40 + DN50 available (HEAD verified)", src))

    # HTML
    status_code, final_url, html, err = _safe_get_html(src)
    res["evidence"].append(("HTML fetch", f"status={status_code} err={err}".strip(), final_url))

    base_url = final_url

    if status_code == 200 and html:
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ", strip=True)
        _apply_text_extraction(res, text, final_url)

        # PDF z produktové stránky (často tam je datasheet/specification)
        pdfs = _find_pdf_links(html, base_url)
        if pdfs:
            pdfs_ranked = sorted(pdfs, key=_score_pdf, reverse=True)[:3]  # vezmeme max 3
            for pdf_url in pdfs_ranked:
                pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS, max_pages=None)
                res["evidence"].append(("PDF url", pdf_url, pdf_url))
                res["evidence"].append(("PDF status", pdf_status, pdf_url))
                if pdf_text:
                    _apply_text_extraction(res, pdf_text, pdf_url)
                # pokud už máme flow i materiál, nemusíme další PDF
                if res.get("flow_rate_lps") is not None and res.get("material_detail") is not None:
                    break

    # Brochure fallback: zkusíme přidat průtok/EN1253/material, pokud pořád chybí
    if (res.get("flow_rate_lps") is None) or (res.get("material_detail") is None) or (res.get("din_en_1253_cert") is None):
        for pdf in BROCHURE_PDFS:
            pdf_text, pdf_status = extract_pdf_text_from_url(pdf, headers=HEADERS, max_pages=None)
            res["evidence"].append(("Brochure status", pdf_status, pdf))
            if pdf_text:
                # bereme větší okno a zkusíme vytáhnout cokoliv relevantního
                _apply_text_extraction(res, pdf_text, pdf)
            if res.get("flow_rate_lps") is not None and res.get("material_detail") is not None:
                break

    return res