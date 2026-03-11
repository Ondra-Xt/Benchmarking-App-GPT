from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple
import json
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

BASE = "https://catalog.geberit.de"
SCOPE = "/de-DE"
SEEDS = [
    f"{BASE}{SCOPE}/search?text=CleanLine",
    f"{BASE}{SCOPE}/produkte?text=CleanLine20",
    f"{BASE}{SCOPE}/produkte?text=CleanLine50",
    f"{BASE}{SCOPE}/produkte?text=CleanLine60",
    f"{BASE}{SCOPE}/produkte?text=CleanLine80",
]

ACCESSORY_RE = re.compile(
    r"verbindungsst[üu]ck|reinigungszubeh[öo]r|ablaufkette|verl[äa]ngerung|zubeh[öo]r|allgemeines\s+zubeh[öo]r|rost|rahmen",
    re.IGNORECASE,
)
CLEANLINE_RE = re.compile(r"cleanline\s*(20|50|60|80)?", re.IGNORECASE)
DRAIN_RE = re.compile(r"duschrinne|duschablauf|duschprofil", re.IGNORECASE)
ROHBAU_RE = re.compile(r"rohbauset|rohbau\s*set|rohbau", re.IGNORECASE)

ARTICLE_RE = re.compile(r"\b(\d{3}\.\d{3}[A-Z0-9\.]*|\d{6,}[A-Z0-9\.]*)\b", re.IGNORECASE)
FLOW_LPS_RE = re.compile(r"(\d+(?:[\.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
FLOW_PREF_RE = re.compile(r"ablaufleistung|abflussleistung", re.IGNORECASE)
DN_PAIR_RE = re.compile(r"\bDN\s*(\d{2,3})\s*/\s*(?:DN\s*)?(\d{2,3})\b", re.IGNORECASE)
DN_SINGLE_RE = re.compile(r"\b(?:nennweite\s*)?DN\s*(\d{2,3})\b", re.IGNORECASE)
HEIGHT_RANGE_RE = re.compile(r"(?:einbauh(?:ö|oe)he|estrichh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm", re.IGNORECASE)
HEIGHT_SINGLE_RE = re.compile(r"(?:einbauh(?:ö|oe)he|estrichh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*mm", re.IGNORECASE)
TRAP_SEAL_RE = re.compile(r"sperrwasserh(?:ö|oe)he|geruchsverschluss|verschlussh(?:ö|oe)he", re.IGNORECASE)
LEN_MM_RE = re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE)
LEN_RANGE_CM_RE = re.compile(r"\b(\d{2,3})\s*[-–]\s*(\d{2,3})\s*cm\b", re.IGNORECASE)
LEN_RANGE_MM_RE = re.compile(r"\b(\d{3,4})\s*[-–]\s*(\d{3,4})\s*mm\b", re.IGNORECASE)


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


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _canonicalize_url(url: str) -> str:
    try:
        p = urlparse((url or "").strip())
    except Exception:
        return (url or "").split("#", 1)[0].split("?", 1)[0]
    path = (p.path or "/").replace("//", "/")
    if path != "/":
        path = path.rstrip("/") + "/"
    return f"{p.scheme}://{p.netloc}{path}"


def _in_scope(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.netloc.endswith("catalog.geberit.de") and (p.path or "").startswith(SCOPE)
    except Exception:
        return False


def _extract_title(html: str, fallback_url: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    h1 = soup.select_one("h1")
    if h1:
        t = _clean_text(h1.get_text(" ", strip=True))
        if t:
            return t
    t = soup.select_one("title")
    if t:
        x = _clean_text(t.get_text(" ", strip=True))
        if x:
            return x
    return fallback_url.rstrip("/").split("/")[-1].replace("-", " ")


def _article_from_text(text: str) -> Optional[str]:
    m = ARTICLE_RE.search(text or "")
    return m.group(1) if m else None


def _normalize_article_for_id(article: str) -> Optional[str]:
    a = re.sub(r"[^0-9A-Za-z]", "", (article or "")).upper()
    return a or None


def _product_id(url: str, text: str) -> str:
    article = _article_from_text(text)
    norm = _normalize_article_for_id(article or "")
    if norm:
        return f"geberit-{norm}"
    return f"geberit-{abs(hash(url))}"


def _length_info(text: str, target_mm: int) -> Tuple[Optional[int], str, Optional[str], bool]:
    for m in LEN_RANGE_CM_RE.finditer(text or ""):
        a = int(m.group(1)) * 10
        b = int(m.group(2)) * 10
        lo, hi = (a, b) if a <= b else (b, a)
        return None, "variable", f"{lo}-{hi} mm", lo <= target_mm <= hi
    for m in LEN_RANGE_MM_RE.finditer(text or ""):
        a = int(m.group(1))
        b = int(m.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        return None, "variable", f"{lo}-{hi} mm", lo <= target_mm <= hi

    vals = []
    for m in LEN_MM_RE.finditer(text or ""):
        v = int(m.group(1))
        if 300 <= v <= 2500:
            vals.append(v)
    if vals:
        return max(vals), "fixed", None, False
    return None, "unknown", None, False


def _extract_flow(flat: str) -> Tuple[List[float], Optional[Tuple[int, int]]]:
    vals: List[float] = []
    first: Optional[Tuple[int, int]] = None
    src = flat or ""

    for km in re.finditer(r"ablaufleistung|abflussleistung", src, re.IGNORECASE):
        lo = max(0, km.start() - 20)
        hi = min(len(src), km.end() + 80)
        part = src[lo:hi]
        for m in FLOW_LPS_RE.finditer(part):
            prev_kw = part.lower().rfind("ablaufleistung", 0, m.start() + 1)
            prev_kw2 = part.lower().rfind("abflussleistung", 0, m.start() + 1)
            prev = max(prev_kw, prev_kw2)
            if prev < 0 or (m.start() - prev) > 35:
                continue
            if "l/s" in part[prev:m.start()].lower():
                continue
            try:
                v = float(m.group(1).replace(",", "."))
            except Exception:
                continue
            if v < 0.10 or v > 3.0:
                continue
            vals.append(v)
            if first is None:
                first = (lo + m.start(), lo + m.end())
    return sorted(set(vals)), first


def _extract_dn(flat: str) -> Tuple[List[str], Optional[Tuple[int, int]]]:
    dns: List[str] = []
    first: Optional[Tuple[int, int]] = None
    for m in DN_PAIR_RE.finditer(flat or ""):
        a = f"DN{m.group(1)}"
        b = f"DN{m.group(2)}"
        for dn in (a, b):
            if dn not in dns:
                dns.append(dn)
        if first is None:
            first = (m.start(), m.end())
    for m in DN_SINGLE_RE.finditer(flat or ""):
        dn = f"DN{m.group(1)}"
        if dn not in dns:
            dns.append(dn)
            if first is None:
                first = (m.start(), m.end())
    return sorted(dns), first


def _extract_height(flat: str) -> Tuple[Optional[int], Optional[int], Optional[Tuple[int, int]]]:
    h = HEIGHT_RANGE_RE.search(flat or "")
    if h and not TRAP_SEAL_RE.search(_snippet(flat, h.start(), h.end(), pad=20)):
        a, b = int(h.group(1)), int(h.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if 20 <= lo <= 350 and 20 <= hi <= 350:
            return lo, hi, (h.start(), h.end())
    hs = HEIGHT_SINGLE_RE.search(flat or "")
    if hs and not TRAP_SEAL_RE.search(_snippet(flat, hs.start(), hs.end(), pad=20)):
        v = int(hs.group(1))
        if 20 <= v <= 350:
            return v, v, (hs.start(), hs.end())
    return None, None, None


def _is_landing(url: str, title: str, flat: str) -> bool:
    txt = f"{url} {title}".lower()
    if "/search" in txt or txt.rstrip("/").endswith("/de-de"):
        return True
    if "cleanline" in txt and not DRAIN_RE.search(txt):
        # keep if technical content exists
        if not (_extract_pairs := ARTICLE_RE.search(flat or "")) and "technische daten" not in (flat or "").lower():
            return True
    return False


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    tol = int(tolerance_mm)
    min_len, max_len = max(0, want - tol), want + tol

    out: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    queue = list(SEEDS)
    seen: Set[str] = set()
    pages: Set[str] = set()

    while queue and len(seen) < 300:
        u = queue.pop(0)
        u = _canonicalize_url(u)
        if u in seen:
            continue
        seen.add(u)

        st, final, html, err = _safe_get_text(u)
        if st != 200 or not html:
            debug.append({"site": "geberit", "seed_url": u, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "crawl", "is_index": None})
            continue

        final_c = _canonicalize_url(final)
        pages.add(final_c)
        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            cand = _canonicalize_url(urljoin(final, a.get("href") or ""))
            if _in_scope(cand) and cand not in seen and cand not in queue:
                queue.append(cand)

    product_urls: List[str] = []
    bom_options_count = 0
    unknown_length_count = 0

    for u in sorted(pages):
        st, final, html, err = _safe_get_text(u)
        if st != 200 or not html:
            continue

        title = _extract_title(html, final)
        flat = _main_flat_text(html)
        txt = f"{title} {flat} {u}".lower()

        if not CLEANLINE_RE.search(txt):
            continue
        if _is_landing(u, title, flat):
            continue

        if ACCESSORY_RE.search(txt):
            out.append({
                "manufacturer": "geberit",
                "product_id": _product_id(u, f"{title} {flat}"),
                "product_family": "CleanLine",
                "product_name": title,
                "product_url": u,
                "sources": u,
                "candidate_type": "component",
                "complete_system": "component",
                "selected_length_mm": want,
                "length_mode": "unknown",
                "length_delta_mm": None,
            })
            continue

        if not DRAIN_RE.search(txt):
            continue

        length_mm, length_mode, range_text, range_match = _length_info(f"{title} {flat} {u}", want)
        if length_mode == "fixed" and length_mm is not None and not (min_len <= length_mm <= max_len):
            continue
        if length_mode == "variable" and not range_match:
            continue

        pid = _product_id(u, f"{title} {flat}")
        out.append({
            "manufacturer": "geberit",
            "product_id": pid,
            "product_family": "CleanLine",
            "product_name": title if length_mm is None else f"{title} ({length_mm} mm)",
            "product_url": u,
            "sources": u,
            "candidate_type": "drain",
            "complete_system": "yes",
            "selected_length_mm": want,
            "length_mode": length_mode,
            "length_delta_mm": None if length_mm is None else (length_mm - want),
            "discovery_evidence": "Length (range)" if length_mode == "variable" and range_text else None,
        })
        if length_mode == "unknown":
            unknown_length_count += 1
        product_urls.append(u)

        # cheap count for summary
        bom_options_count += sum(1 for m in re.finditer(ROHBAU_RE, flat or "", re.IGNORECASE))

    dedup: Dict[str, Dict[str, Any]] = {}
    component_urls: List[str] = []
    for r in out:
        pid = str(r.get("product_id") or "").strip()
        if not pid:
            continue
        if pid not in dedup:
            dedup[pid] = r
            if str(r.get("candidate_type")) == "component":
                component_urls.append(str(r.get("product_url") or ""))

    debug.append({
        "site": "geberit",
        "seed_url": f"{BASE}{SCOPE}",
        "status_code": 200 if dedup else None,
        "final_url": f"{BASE}{SCOPE}",
        "error": "" if dedup else "No accepted candidates.",
        "candidates_found": len(dedup),
        "method": "summary",
        "is_index": None,
        "products_count": sum(1 for r in dedup.values() if str(r.get("candidate_type")) == "drain"),
        "bom_options_count": bom_options_count,
        "unknown_length_count": unknown_length_count,
        "sample_products_urls": json.dumps(product_urls[:10], ensure_ascii=False),
    })

    if component_urls:
        debug.append({
            "site": "geberit",
            "seed_url": f"{BASE}{SCOPE}",
            "status_code": 200,
            "final_url": f"{BASE}{SCOPE}",
            "error": "",
            "candidates_found": len(component_urls),
            "method": "components",
            "is_index": None,
            "sample_components_urls": json.dumps(component_urls[:10], ensure_ascii=False),
        })

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

    src = _canonicalize_url((product_url or "").strip())
    st, final, html, err = _safe_get_text(src)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))
    if st != 200 or not html:
        return res

    flat = _main_flat_text(html)

    m_mat = re.search(r"\b(edelstahl|stahl|kunststoff|metall)\b", flat, re.IGNORECASE)
    if m_mat:
        res["material_detail"] = m_mat.group(1)
        res["material_v4a"] = "yes" if "edelstahl" in m_mat.group(1).lower() else None
        res["evidence"].append(("Material", _snippet(flat, m_mat.start(), m_mat.end()), final))

    len_mm, len_mode, range_txt, _ = _length_info(flat, 1200)
    if len_mm is not None:
        res["resolved_length_mm"] = len_mm
        res["evidence"].append(("Length", f"{len_mm} mm", final))
    elif len_mode == "variable" and range_txt:
        res["evidence"].append(("Length (range)", range_txt, final))

    dns, dn_span = _extract_dn(flat)
    if dns:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        if dn_span:
            res["evidence"].append(("Outlet DN", _snippet(flat, dn_span[0], dn_span[1]), final))

    flow_opts, flow_span = _extract_flow(flat)
    if flow_opts:
        res["flow_rate_lps_options"] = json.dumps(flow_opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(flow_opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"
        if flow_span:
            res["evidence"].append(("Flow rate (Ablaufleistung)", _snippet(flat, flow_span[0], flow_span[1]), final))

    hmin, hmax, hspan = _extract_height(flat)
    if hmin is not None and hmax is not None:
        res["height_adj_min_mm"] = hmin
        res["height_adj_max_mm"] = hmax
        if hspan:
            res["evidence"].append(("Installation height (mm)", _snippet(flat, hspan[0], hspan[1]), final))

    return res


def _parse_rohbauset_page(url: str) -> Optional[Dict[str, Any]]:
    st, final, html, _ = _safe_get_text(url)
    if st != 200 or not html:
        return None
    title = _extract_title(html, final)
    flat = _main_flat_text(html)
    article = _article_from_text(f"{title} {flat}")
    aid = _normalize_article_for_id(article or "")
    dns, dn_span = _extract_dn(flat)
    hmin, hmax, hspan = _extract_height(flat)

    evidence = []
    if dn_span:
        evidence.append(("BOM Outlet DN", _snippet(flat, dn_span[0], dn_span[1]), final))
    if hspan:
        evidence.append(("BOM Installation height", _snippet(flat, hspan[0], hspan[1]), final))

    return {
        "bom_code": f"ROHBAU-{aid}" if aid else (article or title[:40]),
        "bom_name": title,
        "bom_url": final,
        "article_no": article,
        "outlet_dn": "/".join(dns) if dns else None,
        "height_adj_min_mm": hmin,
        "height_adj_max_mm": hmax,
        "is_default": "yes",
        "evidence_json": json.dumps(evidence, ensure_ascii=False) if evidence else None,
    }


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    _ = params
    src = _canonicalize_url((product_url or "").strip())
    st, final, html, _ = _safe_get_text(src)
    if st != 200 or not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    rohbau_links: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        txt = _clean_text(a.get_text(" ", strip=True))
        target = _canonicalize_url(urljoin(final, href))
        if not _in_scope(target):
            continue
        if ROHBAU_RE.search(f"{txt} {target}"):
            rohbau_links.append(target)

    out: List[Dict[str, Any]] = []
    seen = set()
    for u in rohbau_links[:10]:
        if u in seen:
            continue
        seen.add(u)
        parsed = _parse_rohbauset_page(u)
        if parsed:
            out.append(parsed)

    return out
