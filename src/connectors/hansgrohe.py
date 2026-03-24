# src/connectors/hansgrohe.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re
import json
import gzip
from urllib.parse import urljoin, urlparse, urlunparse

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

_LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE | re.DOTALL)


# ----------------------------
# basic helpers
# ----------------------------

def _abs(href: str, base_url: str) -> str:
    return urljoin(base_url, href or "")


def _base_from_url(url: str) -> str:
    try:
        p = urlparse(url)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}"
    except Exception:
        pass
    return BASE


def _canonicalize_hansgrohe_url(url: str) -> str:
    """Normalizuje Hansgrohe URL na .de doménu kvůli konzistentním datům."""
    src = (url or "").strip()
    if not src:
        return src
    try:
        p = urlparse(src)
        host = (p.netloc or "").lower()
        if "hansgrohe" in host and host not in {"hansgrohe.de", "www.hansgrohe.de"}:
            return urlunparse((p.scheme or "https", "www.hansgrohe.de", p.path, p.params, p.query, p.fragment))
    except Exception:
        return src
    return src


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


def _extract_article_no(url: str) -> Optional[str]:
    m = re.search(r"(\d{8})(?:\D*$)", url or "")
    return m.group(1) if m else None


# ----------------------------
# product type detection
# ----------------------------

def _is_finish_set(url: str, title: str = "") -> bool:
    u = (url or "").lower()
    t = (title or "").lower()

    # tvrdé guardy: base-set / uBox nikdy není finish-set
    if "ubox" in u:
        return False
    if any(k in u for k in ["base-set", "grundkoerper", "základní-těleso", "zakladni-teleso"]):
        return False

    # pozitivní detekce finish-setů
    if any(k in u for k in ["fertigset", "finish-set", "finishset"]):
        return True
    if any(k in t for k in ["fertigset", "finish set", "finish-set", "finishset", "vrchní sada", "vrchni sada"]):
        return True

    return False

def _is_raindrain_drain(url: str) -> bool:
    u = (url or "").lower()
    if "articledetail" not in u:
        return False
    if "raindrain" not in u:
        return False
    # jen žlaby
    if any(k in u for k in ["duschrinne", "sprchoveho-zlabu", "sprchového-zlabu"]):
        return True
    return False


def _extract_length_mm_from_url(url: str) -> Optional[int]:
    u = (url or "").lower()
    # typicky "...duschrinne-900-..." nebo "...sprchoveho-zlabu-900-..."
    for pat in [
        r"duschrinne-(\d{3,4})",
        r"sprchoveho-zlabu-(\d{3,4})",
        r"sprchového-zlabu-(\d{3,4})",
    ]:
        m = re.search(pat, u)
        if m:
            try:
                v = int(m.group(1))
                if 300 <= v <= 2000:
                    return v
            except Exception:
                return None
    return None


# ----------------------------
# sitemap crawl
# ----------------------------

def _robots_sitemaps(base_url: str) -> List[str]:
    robots_url = base_url.rstrip("/") + "/robots.txt"
    st, final, txt, _ = _safe_get_text(robots_url, timeout=25)
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

    # gzip?
    if payload[:2] == b"\x1f\x8b":
        try:
            payload = gzip.decompress(payload)
        except Exception:
            return [], False

    txt = payload.decode("utf-8", errors="ignore").strip()
    if not txt:
        return [], False

    # plain list
    if "<" not in txt[:200] and "http" in txt:
        urls = [u.strip() for u in re.split(r"\s+", txt) if u.strip().startswith("http")]
        xmlish = sum(1 for u in urls if u.lower().endswith((".xml", ".xml.gz", ".gz")))
        is_index = (xmlish >= max(1, int(0.6 * len(urls)))) if urls else False
        return urls, is_index

    # xml locs
    locs = [m.group(1).strip() for m in _LOC_RE.finditer(txt) if m.group(1).strip()]
    if not locs:
        return [], False

    is_index = ("<sitemapindex" in txt.lower())
    if not is_index:
        xmlish = sum(1 for u in locs if u.lower().endswith((".xml", ".xml.gz", ".gz")))
        is_index = (xmlish >= max(1, int(0.6 * len(locs))))

    return locs, is_index


def _crawl_sitemaps(start_sitemaps: List[str], max_sitemaps: int = 350, max_pages: int = 200000) -> Tuple[List[str], List[Dict[str, Any]]]:
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
            debug.append({
                "site": "hansgrohe",
                "seed_url": sm,
                "status_code": st,
                "final_url": final,
                "error": err,
                "candidates_found": 0,
                "method": "sitemap",
                "is_index": None,
            })
            continue

        urls, is_index = _extract_sitemap_urls(body)
        debug.append({
            "site": "hansgrohe",
            "seed_url": sm,
            "status_code": st,
            "final_url": final,
            "error": err,
            "candidates_found": len(urls),
            "method": "sitemap",
            "is_index": bool(is_index),
        })

        if not urls:
            continue

        if is_index:
            # prioritizuj “article” sitemapy
            urls_sorted = sorted(urls, key=lambda u: (0 if "article" in u.lower() else 1, u))
            for u in urls_sorted:
                if u not in seen:
                    queue.append(u)
        else:
            pages.extend(urls)

    return list(dict.fromkeys(pages)), debug


# ----------------------------
# HTML -> PDF
# ----------------------------

def _find_pdf_url_in_html(html: str, base_url: str, article_no: Optional[str]) -> Optional[str]:
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    cands: List[Tuple[str, str]] = []

    for a in soup.select("a[href*='.pdf']"):
        href = a.get("href") or ""
        if ".pdf" not in href.lower():
            continue
        txt = a.get_text(" ", strip=True) or ""
        cands.append((_abs(href, base_url), txt))

    if not cands:
        return None

    def score(href: str, txt: str) -> int:
        h = href.lower()
        t = txt.lower()
        s = 0
        if h.endswith(".pdf"):
            s += 2
        if any(k in h or k in t for k in ["product_specification", "produktdatenblatt", "datasheet", "product specification", "technical data"]):
            s += 6
        if article_no and article_no in h:
            s += 5
        if "pdf" in h or "pdf" in t:
            s += 1
        return s

    cands.sort(key=lambda x: score(x[0], x[1]), reverse=True)
    return cands[0][0]


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


def _apply_text_extraction(res: Dict[str, Any], text: str, src: str) -> None:
    if not text:
        return

    flat = _clean_text(text)

    # Flow best
    lps, raw_txt, unit, status = select_flow_rate(flat)
    if lps is not None and res.get("flow_rate_lps") is None:
        res["flow_rate_lps"] = lps
        res["flow_rate_raw_text"] = raw_txt
        res["flow_rate_unit"] = unit
        res["flow_rate_status"] = status
        if raw_txt:
            res["evidence"].append(("Flow rate", raw_txt, src))

    # Flow options -> JSON string
    opts = []
    for m in re.finditer(r"([\d.,]+)\s*(l\s*/\s*min|l\s*/\s*s|l\s*/\s*sek)\b", flat, re.IGNORECASE):
        try:
            v = float(m.group(1).replace(",", "."))
            u = m.group(2).lower().replace(" ", "")
            lps_v = v / 60.0 if "min" in u else v
            if 0.05 <= lps_v <= 5.0:
                opts.append(round(lps_v, 4))
        except Exception:
            pass
    opts = sorted(set(opts))
    if opts and res.get("flow_rate_lps_options") is None:
        res["flow_rate_lps_options"] = json.dumps(opts, ensure_ascii=False)
        res["evidence"].append(("Flow rate options", res["flow_rate_lps_options"], src))

    # Material
    detail, v4a = _material_from_text(flat)
    if detail and res.get("material_detail") is None:
        res["material_detail"] = detail
        res["material_v4a"] = v4a
        res["evidence"].append(("Material", detail, src))

    # EN 1253
    if res.get("din_en_1253_cert") is None:
        if re.search(r"(DIN\s*)?EN\s*1253(\s*[-/]\s*\d+)?", flat, re.IGNORECASE):
            res["din_en_1253_cert"] = "yes"
            res["evidence"].append(("DIN EN 1253", "found", src))

    # DIN/EN 18534
    if res.get("din_18534_compliance") is None:
        if re.search(r"(DIN|EN)\s*18534", flat, re.IGNORECASE):
            res["din_18534_compliance"] = "yes"
            res["evidence"].append(("DIN 18534", "found", src))

    # Height range priority:
    # 1) Installation/Construction keywords
    # 2) Parse first 1-2 valid mm values in the ~220 chars after the first keyword
    # 3) Ignore tile-depth / trap-seal related mm values and tiny values <= 20 mm
    def _extract_height_after_keyword(match_obj: re.Match) -> Optional[Tuple[int, int, str]]:
        seg_start = match_obj.end()
        seg_end = min(len(flat), seg_start + 220)
        seg = flat[seg_start:seg_end]

        # Prefer explicit range first (e.g., 70-95 mm)
        rm = re.search(r"(\d{1,3})\s*[-–]\s*(\d{1,3})\s*mm", seg, re.IGNORECASE)
        if rm:
            try:
                ra = int(rm.group(1))
                rb = int(rm.group(2))
                if 1 <= ra <= 300 and 1 <= rb <= 300:
                    hmin, hmax = (ra, rb) if ra <= rb else (rb, ra)
                    ev_lo = max(0, match_obj.start() - 20)
                    ev_hi = min(len(flat), seg_end)
                    return hmin, hmax, flat[ev_lo:ev_hi]
            except Exception:
                pass

        filtered_vals: List[int] = []
        for vm in re.finditer(r"(\d{1,3})\s*mm", seg, re.IGNORECASE):
            v = int(vm.group(1))

            ctx_start = max(0, vm.start() - 40)
            left_ctx = seg[ctx_start:vm.start()].lower()
            if any(k in left_ctx for k in [
                "fliesenmulde",
                "fliesenmulden",
                "fliesenstärke",
                "fliesenstaerke",
                "sperrwasser",
                "water seal",
            ]):
                continue

            if v <= 20:
                continue
            if not (1 <= v <= 300):
                continue

            filtered_vals.append(v)
            if len(filtered_vals) >= 2:
                break

        if not filtered_vals:
            return None

        hmin = filtered_vals[0]
        hmax = filtered_vals[1] if len(filtered_vals) >= 2 else filtered_vals[0]
        if hmax < hmin:
            hmin, hmax = hmax, hmin
        if not (1 <= hmin <= 300 and 1 <= hmax <= 300):
            return None

        ev_lo = max(0, match_obj.start() - 20)
        ev_hi = min(len(flat), seg_end)
        return hmin, hmax, flat[ev_lo:ev_hi]

    keyword_match = re.search(
        r"(minimale\s+installationsh(?:ö|oe)he|minimal\s+installation\s+height|"
        r"einbauh(?:ö|oe)he|bauh(?:ö|oe)he|aufbauh(?:ö|oe)he)\s*:?",
        flat,
        re.IGNORECASE,
    )

    if keyword_match:
        parsed = _extract_height_after_keyword(keyword_match)
        if parsed:
            h_min, h_max, snippet = parsed
            cur_max = res.get("height_adj_max_mm")
            should_set = (
                res.get("height_adj_min_mm") is None
                or cur_max is None
                or (isinstance(cur_max, (int, float)) and cur_max <= 30)
            )
            if should_set:
                res["height_adj_min_mm"] = h_min
                res["height_adj_max_mm"] = h_max
            res["evidence"].append(("Installation/Construction height (mm)", snippet, src))

    # Explicit minimum construction-height forms (single value), e.g. uBox 01000180
    # - "minimum construction height: XX mm"
    # - "minimale Bauhöhe: XX mm" / "Mindestbauhöhe: XX mm"
    if res.get("height_adj_min_mm") is None or res.get("height_adj_max_mm") is None:
        min_constr = re.search(
            r"(?:minimum\s+construction\s+height|minimale\s+bauh(?:ö|oe)he|mindestbauh(?:ö|oe)he)\s*:?\s*(\d{1,3})\s*mm",
            flat,
            re.IGNORECASE,
        )
        if min_constr:
            try:
                v = int(min_constr.group(1))
            except Exception:
                v = None
            if v is not None and 1 <= v <= 300:
                res["height_adj_min_mm"] = v
                res["height_adj_max_mm"] = v
                lo = max(0, min_constr.start() - 30)
                hi = min(len(flat), min_constr.end() + 60)
                res["evidence"].append(("Installation/Construction height (mm)", flat[lo:hi], src))

    # Trap seal can be useful context, but MUST NOT populate height_adj_*
    trap_match = re.search(r"(sperrwasserh(?:ö|oe)he|water\s+seal\s+height)\s*:?", flat, re.IGNORECASE)
    if trap_match:
        seg_start = trap_match.end()
        seg_end = min(len(flat), seg_start + 220)
        ev_lo = max(0, trap_match.start() - 20)
        ev_hi = min(len(flat), seg_end)
        res["evidence"].append(("Trap seal height (mm)", flat[ev_lo:ev_hi], src))

    # Outlet DN (pokud explicitně v textu)
    if res.get("outlet_dn") is None:
        m = re.search(r"\bDN\s*0?(\d{2,3})\b", flat, re.IGNORECASE)
        if m:
            dn = f"DN{m.group(1)}"
            res["outlet_dn"] = dn
            lo = max(0, m.start() - 60)
            hi = min(len(flat), m.end() + 120)
            res["evidence"].append(("Outlet DN", flat[lo:hi], src))


# ----------------------------
# Public API
# ----------------------------

def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    """
    Discovery:
    - Raindrain žlaby (articledetail + raindrain + duschrinne / sprchový žlab) v délce target±tol
    - + 2 pevně definované uBox universal base-set položky jako Components
    """
    want = int(target_length_mm)
    tol = int(tolerance_mm)
    min_len = max(0, want - tol)
    max_len = want + tol

    debug: List[Dict[str, Any]] = []
    sitemaps = _robots_sitemaps(BASE)
    pages, dbg = _crawl_sitemaps(sitemaps)
    debug.extend(dbg)

    articled = [u for u in pages if "articledetail" in (u or "").lower()]
    out: List[Dict[str, Any]] = []

    # (A) RainDrain drains
    for u in sorted(set(articled)):
        if not _is_raindrain_drain(u):
            continue

        L = _extract_length_mm_from_url(u)
        if L is None:
            continue
        if not (min_len <= L <= max_len):
            continue

        title = u.split("/")[-1].replace("-", " ")
        ct = "finish_set" if _is_finish_set(u, title) else "drain"

        out.append({
            "manufacturer": "hansgrohe",
            "product_family": "RainDrain",
            "product_name": title,
            "product_url": _canonicalize_hansgrohe_url(u),
            "sources": _canonicalize_hansgrohe_url(u),
            "candidate_type": ct,
            "complete_system": "requires_base" if ct == "finish_set" else "yes",
            "selected_length_mm": want,
            "length_mode": "url",
            "length_delta_mm": (L - want),
        })

    # (B) uBox universal base-sets (ONLY these two)
    ubox_items = [
        {
            "url": "https://www.hansgrohe.cz/articledetail-ubox-universal-zakladni-teleso-pro-sprchove-zlaby-pro-plochou-instalaci-01000180",
            "name": "uBox universal – flat installation (DN40) 01000180",
        },
        {
            "url": "https://www.hansgrohe.com/articledetail-ubox-universal-base-set-for-finish-sets-for-standard-installation-01001180",
            "name": "uBox universal – standard installation (DN50) 01001180",
        },
    ]
    for it in ubox_items:
        canonical_url = _canonicalize_hansgrohe_url(it["url"])
        out.append({
            "manufacturer": "hansgrohe",
            "product_family": "uBox",
            "product_name": it["name"],
            "product_url": canonical_url,
            "sources": canonical_url,
            "candidate_type": "base_set",
            "complete_system": "component/base-set",
            "selected_length_mm": want,
            "length_mode": "n/a",
            "length_delta_mm": None,
        })

    debug.append({
        "site": "hansgrohe",
        "seed_url": BASE + "/sitemap.xml",
        "status_code": 200 if out else None,
        "final_url": BASE + "/sitemap.xml",
        "error": "" if out else "No candidates after filters.",
        "candidates_found": len(out),
        "method": "final",
        "is_index": None,
    })

    return out, debug


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    product_url = _canonicalize_hansgrohe_url(product_url)
    title = ""
    if params and isinstance(params, dict):
        title = str(params.get("_title") or params.get("product_name") or "")

    # fallback z URL (důležité)
    if not title:
        title = (product_url or "").split("/")[-1].replace("-", " ")

    if not _is_finish_set(product_url, title):
        return []

    return [
        {
            "bom_code": "UBOX-STD-DN50",
            "bom_name": "uBox universal – standard installation (DN50)",
            "bom_url": "https://www.hansgrohe.com/articledetail-ubox-universal-base-set-for-finish-sets-for-standard-installation-01001180",
            "outlet_dn": "DN50",
            "is_default": "yes",
        },
        {
            "bom_code": "UBOX-FLAT-DN40",
            "bom_name": "uBox universal – flat installation (DN40)",
            "bom_url": "https://www.hansgrohe.de/articledetail-ubox-universal-zakladni-teleso-pro-sprchove-zlaby-pro-plochou-instalaci-01000180",
            "outlet_dn": "DN40",
            "is_default": "no",
        },
    ]

def _extract_height_only_from_source(url: str) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    st, final, html, err = _safe_get_text(url, timeout=35)
    if st != 200 or not html:
        return None, None, None, None

    tmp: Dict[str, Any] = {
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

    pdf_url = _find_pdf_url_in_html(html, base_url=_base_from_url(final), article_no=_extract_article_no(url))
    if pdf_url:
        pdf_text, _ = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
        if pdf_text:
            _apply_text_extraction(tmp, pdf_text, pdf_url)

    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text(" ", strip=True) or ""
    if page_text:
        _apply_text_extraction(tmp, page_text, final)

    snippet = None
    snippet_src = pdf_url or final
    for lbl, snip, ssrc in reversed(tmp.get("evidence") or []):
        if "height" in str(lbl).lower():
            snippet = str(snip)
            snippet_src = str(ssrc)
            break

    return tmp.get("height_adj_min_mm"), tmp.get("height_adj_max_mm"), snippet, snippet_src


def extract_parameters(product_url: str) -> Dict[str, Any]:
    """
    PDF-first:
    - stáhni HTML
    - najdi nejlepší PDF
    - vytěž parametry z PDF + fallback z HTML
    - finish_set DN = DN40/DN50 (default DN50 + options_json)
    """
    res: Dict[str, Any] = {
        "flow_rate_lps": None,
        "flow_rate_raw_text": None,
        "flow_rate_unit": None,
        "flow_rate_status": None,
        "flow_rate_lps_options": None,  # JSON string

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

    src = _canonicalize_hansgrohe_url((product_url or "").strip())
    base_url = _base_from_url(src)
    article_no = _extract_article_no(src)
    title = src.split("/")[-1].replace("-", " ")

    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))

    pdf_url = None
    if st == 200 and html:
        pdf_url = _find_pdf_url_in_html(html, base_url=_base_from_url(final), article_no=article_no)

    # PDF parse
    if pdf_url:
        pdf_text, pdf_status = extract_pdf_text_from_url(pdf_url, headers=HEADERS)
        res["evidence"].append(("PDF url", pdf_url, pdf_url))
        res["evidence"].append(("PDF status", pdf_status, pdf_url))
        if pdf_text:
            _apply_text_extraction(res, pdf_text, pdf_url)

    # HTML fallback parse
    if st == 200 and html:
        soup = BeautifulSoup(html, "lxml")
        page_text = soup.get_text(" ", strip=True) or ""
        if page_text:
            _apply_text_extraction(res, page_text, final)

    # Base-set inheritance for finish_set: if height missing, try default BOM base-set source
    if _is_finish_set(src, title) and (res.get("height_adj_min_mm") is None or res.get("height_adj_max_mm") is None):
        bom_opts = get_bom_options(src, params={"_title": title}) or []
        chosen = None
        for opt in bom_opts:
            if str(opt.get("is_default", "")).lower() == "yes":
                chosen = opt
                break
        if chosen is None:
            for opt in bom_opts:
                if str(opt.get("outlet_dn", "")).upper() == "DN50":
                    chosen = opt
                    break
        if chosen is None and bom_opts:
            chosen = bom_opts[0]

        if chosen and chosen.get("bom_url"):
            hmin, hmax, hsnip, hsrc = _extract_height_only_from_source(str(chosen.get("bom_url")))
            if hmin is not None and hmax is not None:
                res["height_adj_min_mm"] = hmin
                res["height_adj_max_mm"] = hmax
                res["evidence"].append(("Height inherited from base-set", hsnip or f"{hmin}-{hmax} mm", hsrc or str(chosen.get("bom_url"))))

    # DN pravidla pro finish set
    if _is_finish_set(src, title):
        opts = ["DN40", "DN50"]
        res["outlet_dn"] = "DN40/DN50"
        res["outlet_dn_default"] = "DN50"
        res["outlet_dn_options_json"] = json.dumps(opts, ensure_ascii=False)
        res["evidence"].append(("Outlet DN options", res["outlet_dn_options_json"], src))
        res["evidence"].append(("Outlet DN default", "DN50", src))

    # u base_set jen evidence info (DN už je implicitní v BOM / názvu)
    if "ubox" in (src.lower()):
        res["evidence"].append(("Product type", "base_set (uBox universal)", src))

    return res
