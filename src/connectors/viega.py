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

BASE = "https://www.viega.de"
DETAIL_SCOPE = "/de/produkte/Katalog/Entwaesserungstechnik/Advantix-Duschrinnen/"
CATALOG_SEED = f"{BASE}{DETAIL_SCOPE.rstrip('/')}.html"
DETAIL_SEEDS = [
    f"{BASE}{DETAIL_SCOPE}Advantix-Cleviva-Duschrinnen/Einbauhoehe-ab-70-mm/Advantix-Cleviva-Duschrinne-4981-11.html",
    f"{BASE}{DETAIL_SCOPE}Advantix-Vario-Duschrinnen/Einbauhoehe-ab-70-mm/Advantix-Vario-Duschrinnen-4966-10.html",
]

DETAIL_URL_RE = re.compile(r"-\d{4,5}-\d{2}\.html$", re.IGNORECASE)
ARTICLE_FROM_URL_RE = re.compile(r"-(\d{4,5}-\d{2})\.html(?:$|[?#])", re.IGNORECASE)
LENGTH_RE_LIST = [
    re.compile(r"\bl(?:ä|ae)nge\s*(\d{3,4})\s*mm\b", re.IGNORECASE),
    re.compile(r"\bl(?:ä|ae)nge\s*(\d{3,4})\b", re.IGNORECASE),
    re.compile(r"\b(\d)\.(\d{3})\s*mm\b", re.IGNORECASE),
    re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE),
    re.compile(r"\b(\d{3,4})mm\b", re.IGNORECASE),
]
LENGTH_RANGE_RE = re.compile(r"\b(\d{3,4})\s*[-–]\s*(\d{3,4})\s*mm\b", re.IGNORECASE)
VARIABLE_LEN_RE = re.compile(r"\b(vario|variabel|stufenlos|kuerzbar|kürzbar|l\s*\d{3,4}\s*[-–]\s*\d{3,4})\b", re.IGNORECASE)
COMPONENT_KEYWORDS = (
    "zubehoer", "zubehör", "rost", "abdeckung", "einleger", "profil", "rahmen",
    "geruchverschluss", "geruchsverschluss", "ablaufbogen", "ablauf", "siphon",
    "montage", "werkzeug", "schallschutz", "dicht", "fliesen", "adapter",
)

# strict DN parsing; only literal DN and allowed outlet sizes
DN_PAIR_RE = re.compile(r"\bDN\s*(\d{2,3})\s*/\s*(\d{2,3})\b", re.IGNORECASE)
DN_SINGLE_RE = re.compile(r"\b(?:Nennweite\s*)?DN\s*(\d{2,3})\b", re.IGNORECASE)

FLOW_LPS_RE = re.compile(r"(?<!\d)(\d{1,2}(?:[\.,]\d{1,2})?)\s*l/s\b", re.IGNORECASE)
FLOW_REJECT_RE = re.compile(r"reduziert\s+um|reduziert|reduzieren|reduzierung|\bum\b", re.IGNORECASE)
FLOW_PREF_RE = re.compile(r"ablaufleistung|abflussleistung", re.IGNORECASE)

HEIGHT_RE = re.compile(
    r"(?:einbauh(?:ö|oe)he|bauh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm",
    re.IGNORECASE,
)
HEIGHT_SINGLE_RE = re.compile(
    r"(?:einbauh(?:ö|oe)he|bauh(?:ö|oe)he|installationsh(?:ö|oe)he)[^\d]{0,30}(\d{2,3})\s*mm",
    re.IGNORECASE,
)
TRAP_SEAL_RE = re.compile(r"sperrwasserh(?:ö|oe)he|geruchsverschluss|water\s+seal", re.IGNORECASE)


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
        t = _clean_text(h1.get_text(" ", strip=True))
        if t:
            return t
    t = soup.select_one("title")
    if t:
        x = _clean_text(t.get_text(" ", strip=True))
        if x:
            return x
    return fallback_url.rstrip("/").split("/")[-1].replace("-", " ")


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _product_id_from_url(url: str) -> str:
    m = ARTICLE_FROM_URL_RE.search(url or "")
    if m:
        return f"viega-{_digits_only(m.group(1))}"
    d = _digits_only((url or "").split("/")[-1])
    if len(d) >= 6:
        return f"viega-{d}"
    return f"viega-{abs(hash(url or ''))}"


def _abs(href: str, base: str) -> str:
    return urljoin(base, href or "")


def _in_scope(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.netloc.endswith("viega.de") and (p.path or "").startswith(DETAIL_SCOPE)
    except Exception:
        return False


def _is_detail_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return _in_scope(url) and bool(DETAIL_URL_RE.search(p.path or ""))
    except Exception:
        return False


def _is_rost_component(url: str, title: str = "") -> bool:
    txt = f"{url} {title}".lower()
    return "rost" in txt


def _has_component_keyword(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in COMPONENT_KEYWORDS)


def _classify_candidate(url: str, title: str) -> str:
    u = (url or "").lower()
    t = (title or "").lower()
    if "/zubehoer/" in u or "/zubehör/" in u or _has_component_keyword(f"{u} {t}"):
        return "component"
    if ("duschrinne" in t or "duschrinnen" in t) and not _has_component_keyword(t):
        return "drain"
    if "grundkörper" in t or "grundkoerper" in t:
        if "duschrinne" in t or "duschrinnen" in t:
            return "drain"
        return "component"
    return "component"


def _extract_category_links_from_sortiment(html: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html or "", "lxml")
    out: Set[str] = set()

    # try explicit Sortiment section first
    sort_headers = soup.find_all(string=re.compile(r"sortiment", re.IGNORECASE))
    for sh in sort_headers:
        node = sh.parent
        container = node
        for _ in range(4):
            if container is None:
                break
            for a in container.select("a[href]"):
                u = _abs(a.get("href") or "", base_url)
                if _in_scope(u) and not _is_detail_url(u):
                    out.add(u)
            container = container.parent

    # fallback: all in-scope non-detail links from seed
    if not out:
        for a in soup.select("a[href]"):
            u = _abs(a.get("href") or "", base_url)
            if _in_scope(u) and not _is_detail_url(u):
                out.add(u)

    return out


def _crawl_category_pages(start_pages: Set[str], max_pages: int = 2000) -> Set[str]:
    queue = list(start_pages)
    seen: Set[str] = set()
    details: Set[str] = set()

    while queue and len(seen) < max_pages:
        u = queue.pop(0)
        if u in seen:
            continue
        seen.add(u)

        st, final, html, _ = _safe_get_text(u, timeout=30)
        if st != 200 or not html:
            continue

        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            cand = _abs(a.get("href") or "", final)
            if not _in_scope(cand):
                continue
            if _is_detail_url(cand):
                details.add(cand)
            else:
                if cand not in seen and cand not in queue:
                    queue.append(cand)

    return details


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


def _resolve_length_from_text(flat: str) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    if VARIABLE_LEN_RE.search(flat or ""):
        m = VARIABLE_LEN_RE.search(flat or "")
        return None, (_snippet(flat, m.start(), m.end()) if m else "variable length"), "variable"
    opts = _extract_length_options(flat)
    if not opts:
        return None, None, None
    chosen = max(opts)
    m = re.search(rf"\b{chosen}\s*mm\b|\b{chosen}mm\b|\b{str(chosen)[:1]}\.{str(chosen)[1:]}\s*mm\b", flat, re.IGNORECASE)
    return chosen, (_snippet(flat, m.start(), m.end()) if m else f"{chosen} mm"), "fixed"


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


def _extract_dns_from_table(html: str) -> Tuple[List[str], Optional[str]]:
    soup = BeautifulSoup(html or "", "lxml")
    for table in soup.select("table"):
        headers = [_clean_text(th.get_text(" ", strip=True)).lower() for th in table.select("thead th, tr th")]
        if not any("dn" in h for h in headers):
            continue
        for tr in table.select("tr"):
            txt = _clean_text(tr.get_text(" ", strip=True))
            m_pair = re.search(r"\b40\s*/\s*50\b", txt)
            if m_pair:
                return ["DN40", "DN50"], txt
            m_single = re.search(r"\b(?:dn\s*)?(40|50)\b", txt, re.IGNORECASE)
            if m_single:
                return [f"DN{m_single.group(1)}"], txt
    return [], None


def _extract_dns_from_text(flat: str) -> Tuple[List[str], Optional[Tuple[int, int]]]:
    dns: List[str] = []
    first_span: Optional[Tuple[int, int]] = None

    for m in DN_PAIR_RE.finditer(flat):
        a = f"DN{m.group(1)}"
        b = f"DN{m.group(2)}"
        if a not in dns:
            dns.append(a)
        if b not in dns:
            dns.append(b)
        if first_span is None:
            first_span = (m.start(), m.end())

    for m in DN_SINGLE_RE.finditer(flat):
        dn = f"DN{m.group(1)}"
        if dn not in dns:
            dns.append(dn)
            if first_span is None:
                first_span = (m.start(), m.end())

    return sorted(dns), first_span


def _flow_value_if_valid(text: str, m: re.Match[str]) -> Optional[float]:
    try:
        v = float(m.group(1).replace(",", "."))
    except Exception:
        return None
    if v < 0.10 or v > 3.0:
        return None
    ctx = text[max(0, m.start() - 25):min(len(text), m.end() + 25)]
    if FLOW_REJECT_RE.search(ctx):
        return None
    # ignore leading negative values like "-0,8 l/s"
    lead = text[max(0, m.start() - 2):m.start()]
    if "-" in lead:
        return None
    return v


def _extract_flow_from_ablaufleistung(flat: str) -> Tuple[List[float], Optional[Tuple[int, int]]]:
    vals: List[float] = []
    first_span: Optional[Tuple[int, int]] = None

    for seg in re.finditer(r"[^\n\r.;:|]{0,160}(?:ablaufleistung|abflussleistung)[^\n\r.;:|]{0,160}", flat, re.IGNORECASE):
        part = seg.group(0)
        if "l/s" not in part.lower():
            continue
        for m in FLOW_LPS_RE.finditer(part):
            v = _flow_value_if_valid(part, m)
            if v is None:
                continue
            vals.append(v)
            if first_span is None:
                first_span = (seg.start() + m.start(), seg.start() + m.end())

    return sorted(set(vals)), first_span


def _extract_flow_general(flat: str) -> Tuple[List[float], Optional[Tuple[int, int]]]:
    vals: List[float] = []
    first_span: Optional[Tuple[int, int]] = None
    for m in FLOW_LPS_RE.finditer(flat):
        v = _flow_value_if_valid(flat, m)
        if v is None:
            continue
        vals.append(v)
        if first_span is None:
            first_span = (m.start(), m.end())
    return sorted(set(vals)), first_span



def _apply_text_extraction(res: Dict[str, Any], flat: str, src: str, html: str = "") -> None:
    if not flat:
        return

    # DN: table first
    dns: List[str] = []
    dn_span: Optional[Tuple[int, int]] = None
    if html:
        dns_tab, tab_txt = _extract_dns_from_table(html)
        if dns_tab:
            dns = dns_tab
            # find table snippet occurrence in flat for evidence
            if tab_txt:
                idx = flat.lower().find(tab_txt.lower())
                if idx >= 0:
                    dn_span = (idx, min(len(flat), idx + len(tab_txt)))

    if not dns:
        dns, dn_span = _extract_dns_from_text(flat)

    if dns and res.get("outlet_dn") is None:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        if dn_span:
            res["evidence"].append(("Outlet DN", _snippet(flat, dn_span[0], dn_span[1]), src))

    # flow
    flow_opts_abl, flow_span_abl = _extract_flow_from_ablaufleistung(flat)
    flow_opts_gen, flow_span_gen = _extract_flow_general(flat)
    use_abl = len(flow_opts_abl) >= 1
    flow_opts = flow_opts_abl if use_abl else flow_opts_gen
    flow_span = flow_span_abl if use_abl else flow_span_gen
    if flow_opts:
        res["flow_rate_lps_options"] = json.dumps(flow_opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(flow_opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"
        if flow_span:
            label = "Flow rate (Ablaufleistung)" if use_abl else "Flow rate (fallback)"
            res["evidence"].append((label, _snippet(flat, flow_span[0], flow_span[1]), src))
    elif res.get("flow_rate_lps") is None:
        lps, raw, unit, status = select_flow_rate(flat)
        if lps is not None and lps >= 0.10:
            res["flow_rate_lps"] = lps
            res["flow_rate_raw_text"] = raw
            res["flow_rate_unit"] = unit
            res["flow_rate_status"] = status

    # heights (never trap seal)
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
    _ = int(tolerance_mm)

    out: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    discovered: Set[str] = set(DETAIL_SEEDS)
    accepted_urls: List[str] = []
    component_urls: List[str] = []
    product_urls: List[str] = []
    unknown_length_count = 0
    min_len = max(0, want - int(tolerance_mm))
    max_len = want + int(tolerance_mm)

    # Step 1: seed -> Sortiment category links
    st, final, html, err = _safe_get_text(CATALOG_SEED, timeout=35)
    category_links: Set[str] = set()
    if st == 200 and html:
        category_links = _extract_category_links_from_sortiment(html, final)
        debug.append({"site": "viega", "seed_url": CATALOG_SEED, "status_code": st, "final_url": final, "error": err, "candidates_found": len(category_links), "method": "sortiment", "is_index": None})
    else:
        debug.append({"site": "viega", "seed_url": CATALOG_SEED, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "sortiment", "is_index": None})

    # Step 2: category crawl -> detail links
    detail_links = _crawl_category_pages(category_links, max_pages=2000)
    discovered.update(detail_links)
    debug.append({"site": "viega", "seed_url": CATALOG_SEED, "status_code": 200 if detail_links else None, "final_url": CATALOG_SEED, "error": "" if detail_links else "No detail links from categories", "candidates_found": len(detail_links), "method": "category_crawl", "is_index": None})

    for url in sorted(discovered):
        st, final, html, err = _safe_get_text(url, timeout=35)
        title = url.rstrip("/").split("/")[-1].replace("-", " ")
        length = None
        length_kind = None
        length_snip = None

        if st == 200 and html:
            title = _extract_title(html, final)
            flat = _main_flat_text(html)
            length, length_snip, length_kind = _resolve_length_from_text(f"{title} {flat}")

        cand_type = _classify_candidate(url, title)

        # safeguard: /Zubehoer/ should never become products
        if ("/zubehoer/" in url.lower() or "/zubehör/" in url.lower()) and cand_type == "drain":
            cand_type = "component"

        # Ignore non-Rost Zubehör pages to keep discovery noise low
        if ("zubehoer" in url.lower() or "zubehör" in url.lower()) and not _is_rost_component(url, title):
            debug.append({"site": "viega", "seed_url": url, "status_code": st, "final_url": final, "error": "ignored_non_rost_zubehoer", "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        # Apply length filter only for concrete fixed lengths (not unknown/variable)
        if length is not None and length_kind != "variable" and not (min_len <= length <= max_len):
            debug.append({"site": "viega", "seed_url": url, "status_code": st, "final_url": final, "error": "filtered_by_target_length", "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        # For components (rost), append length range if present
        if cand_type == "component" and st == 200 and html:
            mrg = LENGTH_RANGE_RE.search(_main_flat_text(html))
            if mrg:
                title = f"{title} ({mrg.group(1)}–{mrg.group(2)} mm)"

        out.append({
            "manufacturer": "viega",
            "product_id": _product_id_from_url(url),
            "product_family": "Advantix",
            "product_name": title if length is None else f"{title} ({length} mm)",
            "product_url": url,
            "sources": url,
            "candidate_type": cand_type,
            "complete_system": "component" if cand_type == "component" else "yes",
            "selected_length_mm": want,
            "length_mode": "unknown" if length is None else ("variable" if length_kind == "variable" else "html"),
            "length_delta_mm": None if length is None else (length - want),
        })
        accepted_urls.append(url)
        if cand_type == "component":
            component_urls.append(url)
        else:
            product_urls.append(url)
        if length is None:
            unknown_length_count += 1

        debug.append({
            "site": "viega",
            "seed_url": url,
            "status_code": st,
            "final_url": final,
            "error": err if st != 200 else ("length_variable" if length_kind == "variable" else ("length_unknown" if length is None else "")),
            "candidates_found": 1,
            "method": "detail",
            "is_index": None,
        })

    # keep unique product_id to avoid duplicate IDs in exported Products/Components
    dedup: Dict[str, Dict[str, Any]] = {}
    for r in out:
        pid = str(r.get("product_id") or "")
        if pid and pid not in dedup:
            dedup[pid] = r

    debug.append({
        "site": "viega",
        "seed_url": CATALOG_SEED,
        "status_code": 200 if dedup else None,
        "final_url": CATALOG_SEED,
        "error": "" if dedup else "No accepted candidates.",
        "candidates_found": len(dedup),
        "method": "summary",
        "is_index": None,
        "final_count": len(dedup),
        "total_details": len(discovered),
        "products_count": sum(1 for r in dedup.values() if str(r.get("candidate_type",""))=="drain"),
        "components_count": sum(1 for r in dedup.values() if str(r.get("candidate_type",""))=="component"),
        "unknown_length_count": sum(1 for r in dedup.values() if str(r.get("length_mode",""))=="unknown"),
        "sample_accepted_urls": json.dumps(accepted_urls[:10], ensure_ascii=False),
        "sample_products_urls": json.dumps(product_urls[:10], ensure_ascii=False),
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

    src = (product_url or "").strip()
    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))
    if st != 200 or not html:
        return res

    flat = _main_flat_text(html)
    _apply_text_extraction(res, flat, final, html=html)

    length, len_snip, kind = _resolve_length_from_text(flat)
    if length is not None:
        res["resolved_length_mm"] = length
        res["evidence"].append(("Resolved length (mm)", len_snip or f"{length} mm", final))
    elif kind == "variable":
        res["evidence"].append(("Resolved length", "variable length", final))

    # accessory rost range evidence
    if _is_rost_component(src):
        mrg = LENGTH_RANGE_RE.search(flat)
        if mrg:
            res["evidence"].append(("Accessory length range (mm)", _snippet(flat, mrg.start(), mrg.end()), final))

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
                plen, psnip, pkind = _resolve_length_from_text(flat_pdf)
                if plen is not None:
                    res["resolved_length_mm"] = plen
                    res["evidence"].append(("Resolved length (mm)", psnip or f"{plen} mm", pdf_url))
                elif pkind == "variable":
                    res["evidence"].append(("Resolved length", "variable length", pdf_url))

            if res.get("din_en_1253_cert") is None and re.search(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", flat_pdf, re.IGNORECASE):
                res["din_en_1253_cert"] = "yes"

            done = all(res.get(k) is not None for k in ["outlet_dn", "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm", "resolved_length_mm"])
            if done:
                break

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return []
