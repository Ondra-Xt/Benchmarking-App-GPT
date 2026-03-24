from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
import hashlib
import json
import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

CATALOG_SEED = "https://catalog.geberit.de/de-DE/product/PRO_3932352"
CATALOG_SCOPE = "https://catalog.geberit.de/de-DE/product/"

SECTION_STOP_RE = re.compile(r"bestellen|Weitere Produkte", re.IGNORECASE)
EXCLUDE_RE = re.compile(
    r"zubehoer|ersatzteile|rohbauset|installationsrahmen|verbindungsst(?:u|ue)ck|verlaengerung|spare|accessor",
    re.IGNORECASE,
)
SERIES_RE = re.compile(r"cleanline\s*(20|30|50|60|80)", re.IGNORECASE)
DRAIN_RE = re.compile(r"cleanline.*duschrinne|duschrinne.*cleanline", re.IGNORECASE)
PRODUCT_CODE_RE = re.compile(r"/(PRO_\d+)")
ARTICLE_BLOCK_RE = re.compile(
    r"(?:Ersetzt\s+Art\.-Nr\.\s*[0-9A-Z\.]+\s+)?Art\.-Nr\.\s*(?P<article>[0-9A-Z\.]+)(?P<details>.*?)L\s*:?[ ]*(?P<start>\d{2,3})\D{0,4}(?P<end>\d{2,3})\s*cm",
    re.IGNORECASE | re.DOTALL,
)
FLOW_RE = re.compile(r"Ablaufleistung[^\d]{0,20}(\d+(?:[.,]\d+)?)\s*l\s*/\s*s", re.IGNORECASE)
DN_RE = re.compile(r"\bDN\s*(\d{2,3})\b", re.IGNORECASE)
MATERIAL_RE = re.compile(r"(CrNiMo-Stahl\s*1\.4404|Edelstahl|Kunststoff|Stahl)", re.IGNORECASE)
EN1253_RE = re.compile(r"EN\s*1253", re.IGNORECASE)
LENGTH_RANGE_RE = re.compile(r"(?P<start>\d{2,3})\D{0,4}(?P<end>\d{2,3})\s*cm", re.IGNORECASE)
ROHBAU_LINK_RE = re.compile(r"rohbauset", re.IGNORECASE)


def _build_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(HEADERS)
    return session


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        with _build_session() as session:
            response = session.get(url, timeout=timeout, allow_redirects=True)
        return response.status_code, str(response.url), response.text or "", ""
    except Exception as exc:
        return None, url, "", f"{type(exc).__name__}: {exc}"


def _clean_text(value: str) -> str:
    return " ".join((value or "").split())


def _normalize_text(value: str) -> str:
    return (
        (value or "")
        .lower()
        .replace("\u00e4", "ae")
        .replace("\u00f6", "oe")
        .replace("\u00fc", "ue")
        .replace("\u00df", "ss")
    )


def _canonical_fetch_url(url: str) -> str:
    parsed = urlparse((url or "").strip())
    path = (parsed.path or "/").replace("//", "/").rstrip("/")
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def _build_variant_url(base_url: str, article_no: str) -> str:
    parsed = urlparse(_canonical_fetch_url(base_url))
    query = urlencode({"article": article_no})
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", query, ""))


def _split_variant_url(url: str) -> Tuple[str, Optional[str]]:
    parsed = urlparse((url or "").strip())
    article = parse_qs(parsed.query).get("article", [None])[0]
    return _canonical_fetch_url(url), article


def _main_soup(html: str) -> BeautifulSoup:
    soup = BeautifulSoup(html or "", "lxml")
    main = soup.select_one("main")
    return main if main is not None else soup


def _main_text(html: str) -> str:
    main = _main_soup(html)
    for selector in ["header", "nav", "footer", "aside", "script", "style"]:
        for node in main.select(selector):
            node.decompose()
    return _clean_text(main.get_text(" ", strip=True))


def _truncate_for_primary_product(text: str) -> str:
    source = text or ""
    match = SECTION_STOP_RE.search(source)
    if match:
        return source[:match.start()]
    return source


def _extract_title(html: str, fallback_url: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    for selector in ["main h1", "h1", "title"]:
        node = soup.select_one(selector)
        if node:
            text = _clean_text(node.get_text(" ", strip=True))
            if text:
                return text
    return fallback_url.rstrip("/").split("/")[-1]


def _extract_product_code(url: str) -> Optional[str]:
    match = PRODUCT_CODE_RE.search(_canonical_fetch_url(url))
    return match.group(1) if match else None


def _stable_fallback(value: str) -> str:
    return hashlib.sha1((value or "").encode("utf-8")).hexdigest()[:12]


def _series_name(text: str) -> Optional[str]:
    match = SERIES_RE.search(text or "")
    if not match:
        return None
    return f"CleanLine {match.group(1)}"


def _is_catalog_product_url(url: str) -> bool:
    return _canonical_fetch_url(url).startswith(CATALOG_SCOPE)


def _is_cleanline_product_page(title: str, text: str, url: str) -> bool:
    primary = _normalize_text(f"{title} {_truncate_for_primary_product(text)} {url}")
    identity = _normalize_text(f"{title} {url}")
    return _is_catalog_product_url(url) and bool(DRAIN_RE.search(primary)) and not EXCLUDE_RE.search(identity)


def _section_links(html: str, base_url: str, wanted_titles: Sequence[str]) -> List[str]:
    wanted = [_normalize_text(title) for title in wanted_titles]
    main = _main_soup(html)
    links: List[str] = []

    for heading in main.select("h1, h2, h3, h4"):
        heading_text = _normalize_text(_clean_text(heading.get_text(" ", strip=True)))
        if not any(title in heading_text for title in wanted):
            continue
        container = heading.parent if getattr(heading, "parent", None) is not None else heading
        for anchor in container.select("a[href]"):
            href = _canonical_fetch_url(urljoin(base_url, anchor.get("href") or ""))
            if _is_catalog_product_url(href) and href not in links:
                links.append(href)

    return links


def _article_variants(product_text: str) -> List[Dict[str, Any]]:
    variants: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    text = _truncate_for_primary_product(product_text)

    for match in ARTICLE_BLOCK_RE.finditer(text):
        article_no = match.group("article").strip()
        if article_no in seen:
            continue
        seen.add(article_no)

        start_cm = int(match.group("start"))
        end_cm = int(match.group("end"))
        start_mm = start_cm * 10
        end_mm = end_cm * 10
        detail = _clean_text(match.group("details"))
        detail = re.sub(r"^Farbe\s*/\s*Oberfl\w+", "", detail, flags=re.IGNORECASE).strip(" ,:-")

        variants.append({
            "article_no": article_no,
            "length_min_mm": min(start_mm, end_mm),
            "length_mm": max(start_mm, end_mm),
            "detail": detail,
        })

    return variants


def _extract_flow_values(product_text: str) -> Tuple[List[float], Optional[str]]:
    values: List[float] = []
    raw_snippet: Optional[str] = None
    for match in FLOW_RE.finditer(product_text or ""):
        value = float(match.group(1).replace(",", "."))
        if 0.1 <= value <= 5.0 and value not in values:
            values.append(value)
            if raw_snippet is None:
                lo = max(0, match.start() - 120)
                hi = min(len(product_text), match.end() + 120)
                raw_snippet = product_text[lo:hi]
    return values, raw_snippet


def _rohbau_links(product_html: str, product_url: str) -> List[str]:
    return _section_links(product_html, product_url, ["Zusaetzlich zu bestellen"])


def _extract_dn_from_rohbausets(product_html: str, product_url: str) -> Tuple[List[str], List[Tuple[str, str, str]]]:
    dn_values: List[str] = []
    evidence: List[Tuple[str, str, str]] = []

    for rohbau_url in _rohbau_links(product_html, product_url):
        status, final_url, html, error = _safe_get_text(rohbau_url)
        if status != 200 or not html:
            evidence.append(("Outlet DN (linked Rohbauset)", f"status={status} err={error}", final_url))
            continue
        text = _main_text(html)
        for match in DN_RE.finditer(text):
            dn = f"DN{match.group(1)}"
            if dn not in dn_values:
                dn_values.append(dn)
                lo = max(0, match.start() - 80)
                hi = min(len(text), match.end() + 80)
                evidence.append(("Outlet DN (linked Rohbauset)", text[lo:hi], final_url))
    return dn_values, evidence


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    target = int(target_length_mm)
    tolerance = int(tolerance_mm)

    rows: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []

    seed_status, seed_final, seed_html, seed_error = _safe_get_text(CATALOG_SEED)
    debug.append({
        "site": "geberit",
        "seed_url": CATALOG_SEED,
        "status_code": seed_status,
        "final_url": seed_final,
        "error": seed_error,
        "method": "catalog_seed",
        "candidates_found": 0,
        "is_index": None,
    })
    if seed_status != 200 or not seed_html:
        return [], debug

    page_urls = [seed_final]
    for related_url in _section_links(seed_html, seed_final, ["Weitere Produkte"]):
        if related_url not in page_urls:
            page_urls.append(related_url)

    for page_url in page_urls:
        status, final_url, html, error = _safe_get_text(page_url)
        if status != 200 or not html:
            debug.append({
                "site": "geberit",
                "seed_url": page_url,
                "status_code": status,
                "final_url": final_url,
                "error": error,
                "method": "page_fetch",
                "candidates_found": 0,
                "is_index": None,
            })
            continue

        title = _extract_title(html, final_url)
        main_text = _main_text(html)
        if not _is_cleanline_product_page(title, main_text, final_url):
            continue

        flow_values, _ = _extract_flow_values(main_text)
        if not flow_values:
            continue

        series = _series_name(title) or "CleanLine"
        product_code = _extract_product_code(final_url) or _stable_fallback(final_url)
        page_rows = 0

        for variant in _article_variants(main_text):
            length_mm = int(variant["length_mm"])
            length_min_mm = int(variant["length_min_mm"])
            if not (length_min_mm <= target <= length_mm):
                continue
            if abs(length_mm - target) > tolerance and target not in range(length_min_mm, length_mm + 1):
                continue

            article_no = str(variant["article_no"])
            detail = str(variant["detail"] or "").strip()
            suffix = f" - {detail}" if detail else ""
            rows.append({
                "manufacturer": "geberit",
                "product_family": "CleanLine",
                "product_name": f"{title}{suffix}",
                "product_url": _build_variant_url(final_url, article_no),
                "product_id": f"geberit-{product_code.lower()}-{re.sub(r'[^0-9A-Za-z]', '', article_no).lower()}",
                "candidate_type": "drain",
                "complete_system": "yes",
                "product_series": series,
                "length_mm": length_mm,
                "selected_length_mm": target,
                "length_delta_mm": length_mm - target,
                "available_lengths_mm": f"{length_min_mm}-{length_mm}",
                "discovery_evidence": "Length (range)",
            })
            page_rows += 1

        debug.append({
            "site": "geberit",
            "seed_url": page_url,
            "status_code": status,
            "final_url": final_url,
            "error": "",
            "method": "page_filter",
            "candidates_found": page_rows,
            "is_index": None,
        })

    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        product_id = str(row.get("product_id") or "").strip()
        if product_id and product_id not in deduped:
            deduped[product_id] = row

    return list(deduped.values()), debug


def extract_parameters(product_url: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "product_series": None,
        "product_family": "CleanLine",
        "length_mm": None,
        "resolved_length_mm": None,
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

    fetch_url, article_no = _split_variant_url(product_url)
    status, final_url, html, error = _safe_get_text(fetch_url)
    result["evidence"].append(("HTML fetch", f"status={status} err={error}".strip(), final_url))
    if status != 200 or not html:
        return result

    title = _extract_title(html, final_url)
    text = _main_text(html)
    primary_text = _truncate_for_primary_product(text)
    result["product_series"] = _series_name(title)

    for variant in _article_variants(text):
        if article_no and variant["article_no"] != article_no:
            continue
        result["length_mm"] = int(variant["length_mm"])
        result["resolved_length_mm"] = int(variant["length_mm"])
        result["evidence"].append(("Length", f"{variant['length_min_mm']}-{variant['length_mm']} mm", final_url))
        break

    material_match = MATERIAL_RE.search(primary_text)
    if material_match:
        result["material_detail"] = material_match.group(1)
        if "1.4404" in material_match.group(1) or material_match.group(1).lower() == "edelstahl":
            result["material_v4a"] = "yes"
        result["evidence"].append(("Material", material_match.group(1), final_url))

    if EN1253_RE.search(primary_text):
        result["din_en_1253_cert"] = "yes"
        result["evidence"].append(("EN 1253", "Güteüberwacht nach EN 1253-3", final_url))

    flow_values, flow_snippet = _extract_flow_values(primary_text)
    if flow_values:
        result["flow_rate_lps"] = max(flow_values)
        result["flow_rate_raw_text"] = flow_snippet
        result["flow_rate_unit"] = "l/s"
        result["flow_rate_status"] = "ok"
        result["flow_rate_lps_options"] = json.dumps(flow_values, ensure_ascii=False)
        if flow_snippet:
            result["evidence"].append(("Flow rate", flow_snippet, final_url))

    dn_values, dn_evidence = _extract_dn_from_rohbausets(html, final_url)
    if dn_values:
        ordered = sorted(dn_values, key=lambda item: (0 if item == "DN50" else 1, item))
        result["outlet_dn"] = "/".join(ordered)
        result["outlet_dn_default"] = ordered[0]
        result["outlet_dn_options_json"] = json.dumps(ordered, ensure_ascii=False)
    result["evidence"].extend(dn_evidence)

    return result


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    _ = product_url
    _ = params
    return []
