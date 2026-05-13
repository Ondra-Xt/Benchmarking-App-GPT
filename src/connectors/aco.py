from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
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

BASE = "https://www.aco-haustechnik.de"
BASE_CZ = "https://www.aco.cz"
DUSCHRINNEN_SCOPE = "/produkte/badentwaesserung/duschrinnen/"
BADENTWAESSERUNG_SCOPE = "/produkte/badentwaesserung/"
REIHENDUSCH_SCOPE = "/produkte/badentwaesserung/reihenduschrinnen/"
BADABLAEUFE_SCOPE = "/produkte/badentwaesserung/badablaeufe/"
CZ_SCOPE = "/produkty/odvodneni-koupelen/"
SEED_PAGES = [
    f"{BASE}{BADENTWAESSERUNG_SCOPE}",
    f"{BASE}{DUSCHRINNEN_SCOPE}",
    f"{BASE}{REIHENDUSCH_SCOPE}",
    f"{BASE}{BADABLAEUFE_SCOPE}",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-b/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-cplus/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-c/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-eplus/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-mplus/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-splus/",
    f"{BASE}{REIHENDUSCH_SCOPE}aco-showerdrain-public-80/",
    f"{BASE}{REIHENDUSCH_SCOPE}aco-showerdrain-public-110/",
    f"{BASE}{REIHENDUSCH_SCOPE}aco-showerdrain-public-x/",
    f"{BASE}{BADABLAEUFE_SCOPE}aco-easyflow-plus/",
    f"{BASE}{BADABLAEUFE_SCOPE}aco-easyflow/",
    f"{BASE}{BADABLAEUFE_SCOPE}aco-showerpoint/",
    f"{BASE}{BADABLAEUFE_SCOPE}aco-renovierungsablauf-passino/",
    f"{BASE}{BADABLAEUFE_SCOPE}aco-bodenablauf-passavant/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-c/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm-200-mm/",
    f"{BASE}{DUSCHRINNEN_SCOPE}aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/",
    f"{BASE_CZ}{CZ_SCOPE}",
]

ARTICLE_RE = re.compile(r"\b(?:\d{4}\.?\d{2}\.?\d{2}|\d{8})\b")
L1_RE = re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE)
FLOW_LPS_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
FLOW_AT_RE = re.compile(r"(10|20)\s*mm[^\d]{0,20}(\d+(?:[.,]\d+)?)\s*l\s*/\s*s", re.IGNORECASE)
WATER_SEAL_RE = re.compile(r"(?:geruchverschluss|sperrwasserh(?:oe|ö)he)[^\d]{0,20}(\d{2,3})\s*mm", re.IGNORECASE)
HEIGHT_OE_RE = re.compile(
    r"einbauh(?:ö|oe)he[^.]{0,80}oberkante\s+estrich[^\d]{0,20}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm",
    re.IGNORECASE,
)
HEIGHT_RE = re.compile(r"einbauh(?:ö|oe)he[^\d]{0,25}(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm", re.IGNORECASE)
DN_RE = re.compile(r"\bDN\s*(\d{2})\b", re.IGNORECASE)
EN1253_RE = re.compile(r"\b(?:DIN\s*)?EN\s*1253(?:-1)?\b", re.IGNORECASE)
DN_CONTEXT_RE = re.compile(r"ablaufstutzen|ablauf|anschluss|stutzen|\bdn\b", re.IGNORECASE)
FLOW_REJECT_RE = re.compile(r"reduziert|reduzieren|reduziert\s+die\s+abflussleistung", re.IGNORECASE)
ABFLUSS_PREF_RE = re.compile(r"abflusswert|ablaufleistung", re.IGNORECASE)
CATEGORY_PATHS_EXACT = {
    BADENTWAESSERUNG_SCOPE.rstrip("/"),
    DUSCHRINNEN_SCOPE.rstrip("/"),
    REIHENDUSCH_SCOPE.rstrip("/"),
    BADABLAEUFE_SCOPE.rstrip("/"),
}
ALLOWED_DOMAINS = {"aco-haustechnik.de", "www.aco-haustechnik.de", "aco.cz", "www.aco.cz"}
ALLOWED_PREFIXES = [BADENTWAESSERUNG_SCOPE, CZ_SCOPE]

FAMILY_PATTERNS: List[Tuple[str, str]] = [
    ("showerdrain_public_x", r"public[-\s]?x|w[aä]rmetauscher"),
    ("showerdrain_public_110", r"public[-\s]?110"),
    ("showerdrain_public_80", r"public[-\s]?80"),
    ("showerdrain_splus", r"showerdrain[-\s]?s\+|showerdrain-splus"),
    ("showerdrain_cplus", r"showerdrain[-\s]?c\+|showerdrain-cplus"),
    ("showerdrain_c", r"showerdrain[-\s]?c"),
    ("showerdrain_b", r"showerdrain[-\s]?b"),
    ("showerdrain_eplus", r"showerdrain[-\s]?e\+|showerdrain-eplus"),
    ("showerdrain_mplus", r"showerdrain[-\s]?m\+|showerdrain-mplus"),
    ("easyflowplus", r"easyflow\+|easyflow-plus"),
    ("easyflow", r"\beasyflow\b"),
    ("showerpoint", r"showerpoint"),
    ("mg", r"vpusti\s*mg|koupelnov[ée]\s*vpusti"),
    ("passino", r"passino"),
    ("passavant", r"passavant"),
]


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, str(r.url), (r.text or ""), ""
    except Exception as e:
        return None, url, "", f"{type(e).__name__}: {e}"


def _clean_text(s: str) -> str:
    return " ".join((s or "").split())


def _main_flat_text_from_html(html: str) -> str:
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
    title = soup.select_one("title")
    if title:
        t = _clean_text(title.get_text(" ", strip=True))
        if t:
            return t
    return fallback_url.rstrip("/").split("/")[-1].replace("-", " ")


def _digits_only(article_no: str) -> str:
    return re.sub(r"\D", "", article_no or "")

def _normalize_id_token(s: str) -> str:
    txt = (s or "").lower()
    repl = {
        "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
        "č": "c", "ř": "r", "š": "s", "ž": "z", "ý": "y", "á": "a", "í": "i", "é": "e", "ů": "u", "ú": "u", "ň": "n", "ť": "t", "ď": "d",
    }
    for k, v in repl.items():
        txt = txt.replace(k, v)
    txt = re.sub(r"[^a-z0-9]+", "-", txt)
    txt = re.sub(r"-{2,}", "-", txt).strip("-")
    return txt

def _slug_from_url(url: str) -> str:
    p = urlparse(url)
    parts = [x for x in (p.path or "").split("/") if x]
    for seg in reversed(parts):
        seg_n = _normalize_id_token(seg)
        if seg_n and seg_n not in {"produkte", "produkty", "badentwaesserung", "badablaeufe", "duschrinnen", "reihenduschrinnen", "odvodneni-koupelen", "zubehoer"}:
            return seg_n
    return ""

def _stable_aco_id(final_url: str, family: str, role: str, title: str, article_digits: str = "") -> str:
    if article_digits:
        return f"aco-{article_digits}"
    fam = _normalize_id_token(family if family and family != "unknown" else "showerdrain")
    role_n = _normalize_id_token(role or "product")
    slug = _slug_from_url(final_url)
    if not slug:
        slug = _normalize_id_token(title)[:48]
    generic = {"designrost", "design-rost", "design-roste", "aufsatzstuecke", "aufsatzstueck", "komplettablauf", "komplettablaeufe", "einzelablauf", "rinnenkoerper"}
    if slug in generic:
        name_token = _normalize_id_token(title).split("-")
        name_token = "-".join([t for t in name_token if t][:3]) or "item"
        return f"aco-{fam}-{role_n}-{slug}-{name_token}"
    return f"aco-{fam}-{slug}"

def _nominal_length_from_l1(l1_mm: int) -> int:
    return (l1_mm + 15) if (l1_mm % 100 == 85) else l1_mm


def _extract_pairs_from_table(html: str) -> List[Tuple[int, str, str]]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[Tuple[int, str, str]] = []
    seen = set()

    for table in soup.select("table"):
        headers = [_clean_text(th.get_text(" ", strip=True)).lower() for th in table.select("thead th, tr th")]
        header_text = " | ".join(headers)
        if not any(k in header_text for k in ["artikel", "artikel-nr", "artikel nr", "abmessung", "abmessungen", "l1"]):
            continue

        for tr in table.select("tr"):
            cells = tr.select("th, td")
            if not cells:
                continue
            row_texts = [_clean_text(c.get_text(" ", strip=True)) for c in cells]
            row_joined = " | ".join(row_texts)
            l1_match = L1_RE.search(row_joined)
            article_match = ARTICLE_RE.search(row_joined)
            if not l1_match or not article_match:
                continue
            try:
                l1_mm = int(l1_match.group(1))
            except Exception:
                continue
            article_no = article_match.group(0)
            article_digits = _digits_only(article_no)
            if len(article_digits) < 6:
                continue
            key = (l1_mm, article_digits)
            if key in seen:
                continue
            seen.add(key)
            out.append((l1_mm, article_no, article_digits))

    return out




def _extract_article_row_diagnostics_from_table(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "lxml")
    out: List[Dict[str, Any]] = []

    for table in soup.select("table"):
        headers = [_clean_text(th.get_text(" ", strip=True)).lower() for th in table.select("thead th, tr th")]
        header_text = " | ".join(headers)
        if not any(k in header_text for k in ["artikel", "artikel-nr", "artikel nr"]):
            continue

        for tr in table.select("tr"):
            row_text = _clean_text(tr.get_text(" ", strip=True))
            am = ARTICLE_RE.search(row_text)
            if not am:
                continue
            article_no = am.group(0)
            article_digits = _digits_only(article_no)
            lm = L1_RE.search(row_text)
            raw_length_mm: Optional[int] = None
            nominal_length_mm: Optional[int] = None
            if lm:
                try:
                    raw_length_mm = int(lm.group(1))
                    nominal_length_mm = _nominal_length_from_l1(raw_length_mm)
                except Exception:
                    raw_length_mm = None
                    nominal_length_mm = None

            out.append({
                "article_no": article_no,
                "article_digits": article_digits,
                "raw_length_mm": raw_length_mm,
                "nominal_length_mm": nominal_length_mm,
            })

    return out

def _extract_pairs_from_flat_text(flat: str) -> List[Tuple[int, str, str]]:
    # fallback parser from flattened text (still real HTML-based; no local fabricated data)
    out: List[Tuple[int, str, str]] = []
    seen = set()
    for m in re.finditer(r"(\d{3,4})\s*mm[^\n]{0,80}?(\d{4}\.?\d{2}\.?\d{2}|\d{8})", flat, re.IGNORECASE):
        try:
            l1_mm = int(m.group(1))
        except Exception:
            continue
        article_no = m.group(2)
        article_digits = _digits_only(article_no)
        if len(article_digits) < 6:
            continue
        key = (l1_mm, article_digits)
        if key in seen:
            continue
        seen.add(key)
        out.append((l1_mm, article_no, article_digits))
    return out




def _extract_primary_article_and_length(flat: str) -> Tuple[Optional[str], Optional[int]]:
    article_digits: Optional[str] = None
    am = ARTICLE_RE.search(flat or "")
    if am:
        d = _digits_only(am.group(0))
        if len(d) >= 6:
            article_digits = d

    length_mm: Optional[int] = None
    lm = L1_RE.search(flat or "")
    if lm:
        try:
            raw = int(lm.group(1))
            length_mm = _nominal_length_from_l1(raw)
        except Exception:
            length_mm = None

    return article_digits, length_mm

def _abs(href: str, base: str) -> str:
    return urljoin(base, href or "")


def _in_scope(url: str) -> bool:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        if host not in ALLOWED_DOMAINS:
            return False
        path = (p.path or "").lower()
        return any(path.startswith(pref) for pref in ALLOWED_PREFIXES)
    except Exception:
        return False


def _detect_family(url: str, title: str = "") -> str:
    txt = f"{url} {title}".lower()
    for fam, pat in FAMILY_PATTERNS:
        if re.search(pat, txt, re.IGNORECASE):
            return fam
    return "unknown"


def _classify_role(url: str, title: str, html: str, family: str) -> Tuple[str, str]:
    txt = f"{url} {title}".lower()
    if any(k in txt for k in ("designrost", "rost", "abdeckung", "grate")):
        return "grate", "grate_or_cover_tokens"
    if any(k in txt for k in ("rinnenkoerper", "rinnenkörper", "ablaufkoerper", "ablaufkörper", "einzelablauf")):
        return "drain_body", "drain_body_tokens"
    if any(k in txt for k in ("komplettablauf", "public", "showerpoint", "bodenablauf passavant", "renovierungsablauf")):
        return "complete_system", "complete_system_tokens"
    if any(k in txt for k in ("aufsatz", "zubehoer", "zubehör", "keil", "showerstep", "adapter", "rahmen")):
        return "accessory", "accessory_tokens"
    if family != "unknown":
        return "configuration_family", "family_detected"
    return "accessory", "fallback_accessory"


def _is_accessory_page(url: str, title: str = "") -> bool:
    txt = f"{url} {title}".lower()
    return any(k in txt for k in ("zubehoer", "zubehör", "rost", "abdeckung", "rahmen", "designrost", "rahmenprofil"))


def _is_channel_body_page(url: str, title: str = "") -> bool:
    txt = f"{url} {title}".lower()
    return any(k in txt for k in ("rinnenkoerper", "rinnenkörper", "duschrinne", "showerdrain", "einbauhoehe", "einbauhöhe"))





def _canonicalize_url(url: str) -> str:
    try:
        p = urlparse((url or "").strip())
        path = (p.path or "/").replace("//", "/")
        if path != "/":
            path = path.rstrip("/") + "/"
        return f"{p.scheme}://{p.netloc}{path}"
    except Exception:
        return (url or "").split("#", 1)[0].split("?", 1)[0].rstrip("/") + "/"


def _is_category_page(url: str) -> bool:
    try:
        path = (urlparse(url).path or "").rstrip("/")
        return path in CATEGORY_PATHS_EXACT
    except Exception:
        return False


def _looks_like_detail_drain_page(url: str, title: str, html: str) -> bool:
    txt = f"{url} {title}".lower()
    indicates_channel_body = ("rinnenkoerper" in txt) or ("rinnenkörper" in txt)
    if not indicates_channel_body:
        return False

    pairs = _extract_pairs_from_table(html)
    if pairs:
        return True

    flat = _main_flat_text_from_html(html)
    has_h = bool(HEIGHT_OE_RE.search(flat) or HEIGHT_RE.search(flat))
    has_dn = any(m.group(1) in {"40", "50", "70"} and _has_dn_context(flat, m.start(), m.end()) for m in DN_RE.finditer(flat))
    has_flow = any(ABFLUSS_PREF_RE.search(_snippet(flat, m.start(), m.end(), pad=70)) for m in FLOW_LPS_RE.finditer(flat))
    # technical data gate: DN + flow, optionally height
    return has_dn and has_flow and (has_h or has_dn)

def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    want = int(target_length_mm)
    tol = int(tolerance_mm)
    min_len = max(0, want - tol)
    max_len = want + tol

    out: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    queue: List[str] = list(SEED_PAGES)
    seen_pages = set()
    detail_pages = set()
    canonical_seen = set()
    dropped_fragments = 0
    dropped_out_of_scope_count = 0

    # Crawl category/list pages in scope to discover more ranges and detail pages
    while queue and len(seen_pages) < 250:
        page = queue.pop(0)
        if page in seen_pages:
            continue
        seen_pages.add(page)

        st, final, html, err = _safe_get_text(page, timeout=35)
        if st != 200 or not html:
            debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "crawl", "is_index": None})
            continue

        final_c = _canonicalize_url(final)
        if final_c != final:
            dropped_fragments += 1
        if final_c not in canonical_seen:
            canonical_seen.add(final_c)
            detail_pages.add(final_c)

        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            cand = _abs(a.get("href") or "", final)
            if not _in_scope(cand):
                dropped_out_of_scope_count += 1
                continue
            cand_c = _canonicalize_url(cand)
            if cand_c != cand:
                dropped_fragments += 1
            if cand_c not in seen_pages and cand_c not in queue:
                queue.append(cand_c)
            detail_pages.add(cand_c)

    kept_total = 0
    seen_ids = set()
    product_urls: List[str] = []
    component_urls: List[str] = []
    dropped_category_pages = 0
    accepted_product_pages = 0
    article_rows_found = 0

    article_rows_found_total = 0
    article_rows_with_resolved_length = 0
    article_rows_matching_target = 0
    article_rows_rejected_by_length = 0
    article_rows_missing_length = 0
    sample_matching_rows: List[Dict[str, Any]] = []
    sample_rejected_lengths: List[Dict[str, Any]] = []
    sample_missing_length_rows: List[Dict[str, Any]] = []

    emitted_rows = 0
    dropped_missing_product_id = 0
    dropped_missing_url = 0
    total_urls_seen = 0
    dropped_overview_page_count = 0
    urls_by_scope: Dict[str, int] = {}
    candidates_by_family: Dict[str, int] = {}
    candidates_by_role: Dict[str, int] = {}
    accepted_system_family_count = 0
    accepted_article_variant_count = 0
    expected_families = [
        "showerdrain_splus", "showerdrain_cplus", "showerdrain_c", "showerdrain_b",
        "showerdrain_eplus", "showerdrain_mplus", "showerdrain_public_80",
        "showerdrain_public_110", "showerdrain_public_x", "easyflowplus", "easyflow",
        "showerpoint", "mg", "passino", "passavant",
    ]
    sample_accepted_aco_candidates: List[str] = []

    for page in sorted(detail_pages):
        total_urls_seen += 1
        pp = urlparse(page)
        ppath = (pp.path or "").lower()
        if ppath.startswith(DUSCHRINNEN_SCOPE):
            urls_by_scope["duschrinnen"] = urls_by_scope.get("duschrinnen", 0) + 1
        elif ppath.startswith(REIHENDUSCH_SCOPE):
            urls_by_scope["reihenduschrinnen"] = urls_by_scope.get("reihenduschrinnen", 0) + 1
        elif ppath.startswith(BADABLAEUFE_SCOPE):
            urls_by_scope["badablaeufe"] = urls_by_scope.get("badablaeufe", 0) + 1
        elif ppath.startswith(BADENTWAESSERUNG_SCOPE):
            urls_by_scope["badentwaesserung"] = urls_by_scope.get("badentwaesserung", 0) + 1
        elif ppath.startswith(CZ_SCOPE):
            urls_by_scope["cz_odvodneni_koupelen"] = urls_by_scope.get("cz_odvodneni_koupelen", 0) + 1
        else:
            urls_by_scope["other"] = urls_by_scope.get("other", 0) + 1

        # reject category/landing pages from candidates
        if _is_category_page(page):
            dropped_category_pages += 1
            debug.append({"site": "aco", "seed_url": page, "status_code": 200, "final_url": page, "error": "dropped_category_page", "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        st, final, html, err = _safe_get_text(page, timeout=35)
        if st != 200 or not html:
            debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final, "error": err, "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        final_c = _canonicalize_url(final)
        title_base = _extract_title(html, final_c)
        family = _detect_family(final_c, title_base)
        role, role_reason = _classify_role(final_c, title_base, html, family)

        # route candidate type
        if _is_accessory_page(final_c, title_base):
            cand_type = "component"
        elif _looks_like_detail_drain_page(final_c, title_base, html):
            cand_type = "drain"
        elif family != "unknown":
            cand_type = "component"
        else:
            dropped_overview_page_count += 1
            debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final_c, "error": "dropped_overview_page", "candidates_found": 0, "method": "detail", "is_index": None})
            continue

        pairs = _extract_pairs_from_table(html)
        method = "table" if pairs else "detail_only"

        kept = 0
        if cand_type == "drain":
            # row-based product variants only; no page-level drain fallback rows
            if not pairs:
                # keep family-level candidate instead of dropping entire family due missing row table
                pid = _stable_aco_id(final_c, family, "configuration_family", title_base)
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    kept += 1
                    kept_total += 1
                    out.append({
                        "manufacturer": "aco",
                        "product_id": pid,
                        "product_family": family if family != "unknown" else "ShowerDrain",
                        "product_name": title_base,
                        "product_url": final_c,
                        "sources": final_c,
                        "candidate_type": "component",
                        "system_role": "configuration_family",
                        "classification_reason": "no_article_rows_keep_family",
                        "complete_system": "component",
                        "selected_length_mm": want,
                        "length_mode": "unknown",
                        "length_delta_mm": None,
                    })
                    accepted_system_family_count += 1
                    candidates_by_family[family] = candidates_by_family.get(family, 0) + 1
                    candidates_by_role["configuration_family"] = candidates_by_role.get("configuration_family", 0) + 1
                debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final_c, "error": "no_article_rows", "candidates_found": kept, "method": method, "is_index": None})
                continue

            accepted_product_pages += 1
            article_rows_found += len(pairs)

            diag_rows = _extract_article_row_diagnostics_from_table(html)
            if not diag_rows:
                diag_rows = [
                    {"article_no": a, "article_digits": d, "raw_length_mm": l1, "nominal_length_mm": _nominal_length_from_l1(l1)}
                    for l1, a, d in pairs
                ]

            for row in diag_rows:
                article_rows_found_total += 1
                raw_len = row.get("raw_length_mm")
                nom_len = row.get("nominal_length_mm")
                if nom_len is None:
                    article_rows_missing_length += 1
                    if len(sample_missing_length_rows) < 10:
                        sample_missing_length_rows.append({
                            "page": final_c,
                            "article_no": row.get("article_no"),
                            "raw_length_mm": raw_len,
                            "nominal_length_mm": nom_len,
                            "matched_target": False,
                        })
                    continue

                article_rows_with_resolved_length += 1
                in_target = (min_len <= int(nom_len) <= max_len)
                if in_target:
                    article_rows_matching_target += 1
                    if len(sample_matching_rows) < 10:
                        sample_matching_rows.append({
                            "page": final_c,
                            "article_no": row.get("article_no"),
                            "raw_length_mm": raw_len,
                            "nominal_length_mm": nom_len,
                            "matched_target": True,
                        })
                else:
                    article_rows_rejected_by_length += 1
                    if len(sample_rejected_lengths) < 10:
                        sample_rejected_lengths.append({
                            "page": final_c,
                            "article_no": row.get("article_no"),
                            "raw_length_mm": raw_len,
                            "nominal_length_mm": nom_len,
                            "matched_target": False,
                        })

            for l1_mm, article_no, article_digits in pairs:
                nominal_length_mm = _nominal_length_from_l1(l1_mm)
                # row must have concrete length
                if nominal_length_mm is None:
                    continue
                if not (min_len <= nominal_length_mm <= max_len):
                    continue
                pid = _stable_aco_id(final_c, family, "drain_unit", title_base, article_digits)
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                kept += 1
                kept_total += 1
                out.append({
                    "manufacturer": "aco",
                    "product_id": pid,
                    "product_family": family if family != "unknown" else "ShowerDrain",
                    "product_name": f"{title_base} {nominal_length_mm} mm (Artikel-Nr. {article_no})",
                    "product_url": f"{final_c}#article-{article_digits}",
                    "sources": final_c,
                    "candidate_type": "drain",
                    "system_role": "drain_unit",
                    "classification_reason": "article_row_variant",
                    "complete_system": "yes",
                    "selected_length_mm": want,
                    "length_mode": "L1_nominal_heuristic",
                    "length_delta_mm": nominal_length_mm - want,
                    "article_no": article_no,
                    "row_length_raw_mm": l1_mm,
                    "row_length_nominal_mm": nominal_length_mm,
                })
                accepted_article_variant_count += 1
                candidates_by_family[family] = candidates_by_family.get(family, 0) + 1
                candidates_by_role["drain_unit"] = candidates_by_role.get("drain_unit", 0) + 1
                if len(sample_accepted_aco_candidates) < 20:
                    sample_accepted_aco_candidates.append(f"{pid}|{family}|drain_unit")
                product_urls.append(final_c)
                emitted_rows += 1
        elif cand_type == "component":
            pid = _stable_aco_id(final_c, family, role, title_base)
            if pid not in seen_ids:
                seen_ids.add(pid)
                kept += 1
                kept_total += 1
                out.append({
                    "manufacturer": "aco",
                    "product_id": pid,
                    "product_family": family if family != "unknown" else "ShowerDrain",
                    "product_name": title_base,
                    "product_url": final_c,
                    "sources": final_c,
                    "candidate_type": "component",
                    "system_role": role,
                    "classification_reason": role_reason,
                    "complete_system": "component",
                    "selected_length_mm": want,
                    "length_mode": "unknown",
                    "length_delta_mm": None,
                })
                candidates_by_family[family] = candidates_by_family.get(family, 0) + 1
                candidates_by_role[role] = candidates_by_role.get(role, 0) + 1
                if role == "configuration_family":
                    accepted_system_family_count += 1
                if len(sample_accepted_aco_candidates) < 20:
                    sample_accepted_aco_candidates.append(f"{pid}|{family}|{role}")
                component_urls.append(final_c)
                emitted_rows += 1

        debug.append({"site": "aco", "seed_url": page, "status_code": st, "final_url": final_c, "error": err, "candidates_found": kept, "method": method, "is_index": None})

    # final safety guard: never emit invalid product rows
    safe_out: List[Dict[str, Any]] = []
    for r in out:
        pid = str(r.get("product_id") or "").strip()
        url = str(r.get("product_url") or "").strip()
        if not pid:
            dropped_missing_product_id += 1
            continue
        if pid.lower() == "nan":
            dropped_missing_product_id += 1
            continue
        if not url:
            dropped_missing_url += 1
            continue
        safe_out.append(r)

    # dedupe by stable product_id (not page URL)
    dedup: Dict[str, Dict[str, Any]] = {}
    for r in safe_out:
        pid = str(r.get("product_id") or "").strip()
        if pid and pid not in dedup:
            dedup[pid] = r
    out = list(dedup.values())

    debug.append({
        "site": "aco",
        "seed_url": f"{BASE}{DUSCHRINNEN_SCOPE}",
        "status_code": 200 if out else None,
        "final_url": f"{BASE}{DUSCHRINNEN_SCOPE}",
        "error": "" if out else "No accepted candidates.",
        "candidates_found": len(out),
        "method": "summary",
        "is_index": None,
        "products_count": sum(1 for r in out if str(r.get("candidate_type")) == "drain"),
        "components_count": sum(1 for r in out if str(r.get("candidate_type")) == "component"),
        "sample_products_urls": json.dumps(product_urls[:10], ensure_ascii=False),
        "sample_product_urls": json.dumps(product_urls[:10], ensure_ascii=False),
        "sample_components_urls": json.dumps(component_urls[:10], ensure_ascii=False),
        "total_urls": len(detail_pages),
        "total_urls_seen": total_urls_seen,
        "urls_by_scope": json.dumps(urls_by_scope, ensure_ascii=False),
        "candidates_by_family": json.dumps(candidates_by_family, ensure_ascii=False),
        "candidates_by_role": json.dumps(candidates_by_role, ensure_ascii=False),
        "dropped_out_of_scope_count": dropped_out_of_scope_count,
        "dropped_overview_page_count": dropped_overview_page_count,
        "accepted_system_family_count": accepted_system_family_count,
        "accepted_article_variant_count": accepted_article_variant_count,
        "expected_family_coverage": json.dumps({f: (candidates_by_family.get(f, 0) > 0) for f in expected_families}, ensure_ascii=False),
        "sample_missing_expected_families": json.dumps([f for f in expected_families if candidates_by_family.get(f, 0) <= 0][:10], ensure_ascii=False),
        "sample_accepted_aco_candidates": json.dumps(sample_accepted_aco_candidates[:10], ensure_ascii=False),
        "after_canonicalize": len(canonical_seen),
        "dropped_fragments": dropped_fragments,
        "dropped_category_pages": dropped_category_pages,
        "accepted_products": sum(1 for r in out if str(r.get("candidate_type")) == "drain"),
        "accepted_components": sum(1 for r in out if str(r.get("candidate_type")) == "component"),
        "emitted_rows": emitted_rows,
        "dropped_missing_product_id": dropped_missing_product_id,
        "dropped_missing_url": dropped_missing_url,
        "unknown_length_count": sum(1 for r in out if str(r.get("candidate_type")) == "drain" and str(r.get("length_mode")) == "unknown"),
        "accepted_product_pages": accepted_product_pages,
        "article_rows_found": article_rows_found,
        "article_rows_found_total": article_rows_found_total,
        "article_rows_with_resolved_length": article_rows_with_resolved_length,
        "article_rows_matching_target": article_rows_matching_target,
        "article_rows_rejected_by_length": article_rows_rejected_by_length,
        "article_rows_missing_length": article_rows_missing_length,
        "sample_matching_rows": json.dumps(sample_matching_rows, ensure_ascii=False),
        "sample_rejected_lengths": json.dumps(sample_rejected_lengths, ensure_ascii=False),
        "sample_missing_length_rows": json.dumps(sample_missing_length_rows, ensure_ascii=False),
    })

    return out, debug


def _snippet(flat: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(flat), end + pad)
    return flat[lo:hi]


def _has_dn_context(flat: str, start: int, end: int, window: int = 60) -> bool:
    lo = max(0, start - window)
    hi = min(len(flat), end + window)
    return bool(DN_CONTEXT_RE.search(flat[lo:hi]))


def _is_valid_flow_context(flat: str, start: int, end: int) -> bool:
    # look mostly at nearby prefix where reduction phrasing appears
    prefix = flat[max(0, start - 45):start].lower()
    if FLOW_REJECT_RE.search(prefix):
        return False
    # reject phrasing like "um 0,12 l/s"
    short_prefix = flat[max(0, start - 6):start].lower()
    if short_prefix.endswith(" um ") or short_prefix.endswith("um "):
        return False
    # reject leading minus sign right before number (e.g. "-0,09 l/s")
    lead = flat[max(0, start - 3):start]
    if "-" in lead:
        return False
    return True


def extract_parameters(product_url: str) -> Dict[str, Any]:
    res: Dict[str, Any] = {
        "flow_rate_lps": None,
        "flow_rate_10mm_lps": None,
        "flow_rate_20mm_lps": None,
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
        "water_seal_mm": None,
        "outlet_dn": None,
        "outlet_dn_default": None,
        "outlet_dn_options_json": None,
        "sealing_fleece_preassembled": None,
        "colours_count": None,
        "evidence": [],
    }

    src_full = (product_url or "").strip()
    src = src_full.split("#", 1)[0].strip()
    article_token = ""
    if "#" in src_full:
        frag = src_full.split("#", 1)[1]
        mfrag = re.search(r"(\d{6,10})", frag)
        if mfrag:
            article_token = mfrag.group(1)
    st, final, html, err = _safe_get_text(src, timeout=35)
    res["evidence"].append(("HTML fetch", f"status={st} err={err}".strip(), final))
    if st != 200 or not html:
        return res

    flat = _main_flat_text_from_html(html)

    article_row_matched = False
    article_row_explicit_flow = False
    # article-row specific extraction if article token is available from discovery URL anchor
    if article_token:
        soup = BeautifulSoup(html or "", "lxml")
        for table in soup.select("table"):
            rows = table.select("tr")
            if not rows:
                continue
            header_cells = [(_clean_text(c.get_text(" ", strip=True)).lower()) for c in rows[0].select("th,td")]
            idx_10 = next((i for i, h in enumerate(header_cells) if "10" in h and "mm" in h and ("abfluss" in h or "ablauf" in h)), None)
            idx_20 = next((i for i, h in enumerate(header_cells) if "20" in h and "mm" in h and ("abfluss" in h or "ablauf" in h)), None)
            idx_ws = next((i for i, h in enumerate(header_cells) if "geruch" in h or "sperrwasser" in h), None)
            for tr in rows[1:]:
                cells = tr.select("th,td")
                row_text = _clean_text(tr.get_text(" ", strip=True))
                if article_token not in _digits_only(row_text):
                    continue
                article_row_matched = True
                def _parse_flow_from_cell(ix):
                    if ix is None or ix >= len(cells):
                        return None
                    cm = FLOW_LPS_RE.search(_clean_text(cells[ix].get_text(" ", strip=True)))
                    if not cm:
                        return None
                    try:
                        fv = float(cm.group(1).replace(",", "."))
                        return fv if 0.10 <= fv <= 3.0 else None
                    except Exception:
                        return None
                f10 = _parse_flow_from_cell(idx_10)
                f20 = _parse_flow_from_cell(idx_20)
                if f10 is not None:
                    res["flow_rate_10mm_lps"] = f10
                if f20 is not None:
                    res["flow_rate_20mm_lps"] = f20
                row_has_hydraulic = (f10 is not None) or (f20 is not None)
                article_row_explicit_flow = article_row_explicit_flow or (f10 is not None) or (f20 is not None)
                if idx_ws is not None and idx_ws < len(cells):
                    wsm = re.search(r"(\d{2,3})\s*mm", _clean_text(cells[idx_ws].get_text(" ", strip=True)), re.IGNORECASE)
                    if wsm:
                        try:
                            ws = int(wsm.group(1))
                            if 20 <= ws <= 100:
                                res["water_seal_mm"] = ws
                                row_has_hydraulic = True
                        except Exception:
                            pass
                flows = []
                for m in FLOW_LPS_RE.finditer(row_text):
                    try:
                        fv = float(m.group(1).replace(",", "."))
                    except Exception:
                        continue
                    if 0.10 <= fv <= 3.0:
                        flows.append(fv)
                if flows:
                    res["flow_rate_lps"] = max(flows)
                    res["flow_rate_unit"] = "l/s"
                    res["flow_rate_status"] = "ok"
                    article_row_explicit_flow = True
                res["evidence"].append(("Article row", row_text[:280], final))
                if not row_has_hydraulic:
                    res["evidence"].append(("Article row hydraulics", "article row contains dimensions/price style data but no explicit 10mm/20mm flow or water seal field", final))
                break

    wsm_page = WATER_SEAL_RE.search(flat)
    if wsm_page:
        try:
            ws = int(wsm_page.group(1))
            if 20 <= ws <= 100:
                res["water_seal_mm"] = ws
                res["evidence"].append(("Sperrwasserhöhe (mm)", _snippet(flat, wsm_page.start(), wsm_page.end()), final))
        except Exception:
            pass

    # height (prefer Oberkante Estrich phrase)
    hm = HEIGHT_OE_RE.search(flat) or HEIGHT_RE.search(flat)
    if hm:
        a = int(hm.group(1))
        b = int(hm.group(2))
        lo, hi = (a, b) if a <= b else (b, a)
        if 20 <= lo <= 300 and 20 <= hi <= 300:
            res["height_adj_min_mm"] = lo
            res["height_adj_max_mm"] = hi
            res["evidence"].append(("Einbauhöhe (mm)", _snippet(flat, hm.start(), hm.end()), final))

    # DN
    dns = sorted(
        {
            f"DN{int(m.group(1))}"
            for m in DN_RE.finditer(flat)
            if m.group(1) in {"40", "50", "70"} and _has_dn_context(flat, m.start(), m.end())
        }
    )
    if dns:
        res["outlet_dn"] = "/".join(dns)
        res["outlet_dn_default"] = "DN50" if "DN50" in dns else dns[0]
        res["outlet_dn_options_json"] = json.dumps(dns, ensure_ascii=False)
        first = DN_RE.search(flat)
        if first:
            res["evidence"].append(("Outlet DN", _snippet(flat, first.start(), first.end()), final))

    # flow rate options from Abflusswert/Ablaufleistung snippets only
    lps_values: List[float] = []
    for m in FLOW_LPS_RE.finditer(flat):
        if not _is_valid_flow_context(flat, m.start(), m.end()):
            continue
        ctx = _snippet(flat, m.start(), m.end(), pad=70)
        if not ABFLUSS_PREF_RE.search(ctx):
            continue
        try:
            v = float(m.group(1).replace(",", "."))
        except Exception:
            continue
        if 0.10 <= v <= 3.0:
            lps_values.append(v)
            res["evidence"].append(("Flow rate option (Abflusswert l/s)", _snippet(flat, m.start(), m.end()), final))

    if article_token and article_row_matched and not article_row_explicit_flow:
        res["evidence"].append(("Flow attribution limited", "article row has no explicit 10mm/20mm hydraulic columns; flow may come from generic page-level statement", final))

    if lps_values:
        opts = sorted(set(lps_values))
        res["flow_rate_lps_options"] = json.dumps(opts, ensure_ascii=False)
        res["flow_rate_lps"] = max(opts)
        res["flow_rate_unit"] = "l/s"
        res["flow_rate_status"] = "ok"

    enm = EN1253_RE.search(flat)
    if enm:
        res["din_en_1253_cert"] = "yes"
        res["evidence"].append(("DIN EN 1253", _snippet(flat, enm.start(), enm.end()), final))

    return res


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    src = (product_url or "").split("#", 1)[0].strip()
    st, final, html, err = _safe_get_text(src, timeout=35)
    if st != 200 or not html:
        return []

    soup = BeautifulSoup(html or "", "lxml")
    title = _extract_title(html, final)
    family = _detect_family(final, title)
    if family != "showerdrain_splus":
        return []

    options: List[Dict[str, Any]] = []
    seen = set()
    compatibility_sections = soup.find_all(string=re.compile(r"kompatibel|geeignet\s+f[üu]r|passend\s+zu", re.IGNORECASE))
    if not compatibility_sections:
        return []

    for a in soup.select("a[href]"):
        href = _abs(a.get("href") or "", final)
        if not _in_scope(href):
            continue
        link_txt = _clean_text(a.get_text(" ", strip=True))
        role, _reason = _classify_role(href, link_txt, "", family)
        if role not in {"drain_body", "grate", "accessory"}:
            continue
        comp_id = _stable_aco_id(href, family, role, link_txt)
        key = (comp_id, role)
        if key in seen:
            continue
        seen.add(key)
        option_type = "compatible_drain_body" if role == "drain_body" else ("compatible_grate" if role == "grate" else "optional_accessory")
        options.append({
            "component_id": comp_id,
            "option_type": option_type,
            "option_role": role,
            "option_family": family,
            "parent_family": family,
            "source_url": href,
            "option_label": link_txt[:140],
            "option_meta": "official_splus_compatibility_section",
        })
    return options
