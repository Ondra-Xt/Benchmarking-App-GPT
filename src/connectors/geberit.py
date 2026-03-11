from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
import hashlib
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

MARKETING_SEED = "https://www.geberit.de/badezimmerprodukte/duschen-badewannenablaeufe/duschen/duschrinnen-geberit-cleanline/"
CATALOG_SEED = "https://catalog.geberit.de/de-DE/product/PRO_3932352"
CATALOG_SCOPE = "https://catalog.geberit.de/de-DE/product/"
MARKETING_MODELS = {"cleanline20", "cleanline30", "cleanline50", "cleanline60", "cleanline80"}

ACCESSORY_RE = re.compile(
    r"verbindungsst(?:u|ue)ck|verlaengerung|reinigungszubehoer|allgemeines\s+zubehoer|\bzubehoer\b|ablaufkette",
    re.IGNORECASE,
)
CLEANLINE_RE = re.compile(r"\bcleanline\s*(20|30|50|60|80)\b", re.IGNORECASE)
DRAIN_RE = re.compile(r"duschrinne|duschprofil|duschablauf", re.IGNORECASE)
ROHBAU_RE = re.compile(r"rohbauset|rohbau\s*set|rohbau", re.IGNORECASE)
FLOW_CONTEXT_RE = re.compile(r"ablaufleistung|abflussleistung|durchfluss", re.IGNORECASE)
INSTALL_CONTEXT_RE = re.compile(
    r"einbauh(?:o|oe)he|estrichh(?:o|oe)he|installationsh(?:o|oe)he|bodenaufbau",
    re.IGNORECASE,
)
TRAP_SEAL_RE = re.compile(r"sperrwasserh(?:o|oe)he|geruchsverschluss|verschlussh(?:o|oe)he", re.IGNORECASE)

ARTICLE_RE = re.compile(r"\b(?:art(?:ikel)?(?:-|\s)?nr\.?|artikelnummer|nr\.?)\s*[:#]?\s*([0-9][0-9A-Za-z\.\-]{5,})", re.IGNORECASE)
ARTICLE_FALLBACK_RE = re.compile(r"\b(\d{3}\.\d{3}\.\d{2,3}|\d{6,}[A-Z0-9\.]*)\b", re.IGNORECASE)
FLOW_LPS_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*l\s*/\s*s\b", re.IGNORECASE)
DN_PAIR_RE = re.compile(r"\bDN\s*(\d{2,3})\s*/\s*(?:DN\s*)?(\d{2,3})\b", re.IGNORECASE)
DN_SINGLE_RE = re.compile(r"\bDN\s*(\d{2,3})\b", re.IGNORECASE)
HEIGHT_RANGE_RE = re.compile(r"(\d{2,3})\s*[-–]\s*(\d{2,3})\s*mm", re.IGNORECASE)
HEIGHT_SINGLE_RE = re.compile(r"\b(\d{2,3})\s*mm\b", re.IGNORECASE)
LEN_RANGE_CM_RE = re.compile(r"\b(\d{2,3})\s*[-–]\s*(\d{2,3})\s*cm\b", re.IGNORECASE)
LEN_RANGE_MM_RE = re.compile(r"\b(\d{3,4})\s*[-–]\s*(\d{3,4})\s*mm\b", re.IGNORECASE)
LEN_MM_RE = re.compile(r"\b(\d{3,4})\s*mm\b", re.IGNORECASE)


def _safe_get_text(url: str, timeout: int = 35) -> Tuple[Optional[int], str, str, str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return response.status_code, str(response.url), (response.text or ""), ""
    except Exception as exc:
        return None, url, "", f"{type(exc).__name__}: {exc}"


def _clean_text(value: str) -> str:
    return " ".join((value or "").split())


def _normalize_lookup(value: str) -> str:
    text = (value or "").lower()
    return (
        text.replace("\u00e4", "ae")
        .replace("\u00f6", "oe")
        .replace("\u00fc", "ue")
        .replace("\u00df", "ss")
    )


def _canonicalize_url(url: str) -> str:
    try:
        parsed = urlparse((url or "").strip())
    except Exception:
        return (url or "").split("#", 1)[0].split("?", 1)[0]
    path = (parsed.path or "/").replace("//", "/").rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _catalog_product_url(url: str) -> bool:
    return _canonicalize_url(url).startswith(CATALOG_SCOPE)


def _main_node(html: str):
    soup = BeautifulSoup(html or "", "lxml")
    main = soup.select_one("main")
    return main if main is not None else soup


def _main_flat_text(html: str) -> str:
    main = _main_node(html)
    for selector in ["header", "nav", "footer", "aside", "script", "style"]:
        for tag in main.select(selector):
            tag.decompose()
    return _clean_text(main.get_text(" ", strip=True))


def _extract_title(html: str, fallback_url: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    for selector in ["main h1", "h1", "title"]:
        node = soup.select_one(selector)
        if node:
            text = _clean_text(node.get_text(" ", strip=True))
            if text:
                return text
    return fallback_url.rstrip("/").split("/")[-1]


def _snippet(text: str, start: int, end: int, pad: int = 80) -> str:
    lo = max(0, start - pad)
    hi = min(len(text), end + pad)
    return text[lo:hi]


def _stable_hash(value: str) -> str:
    return hashlib.sha1((value or "").encode("utf-8")).hexdigest()[:12]


def _normalize_article(article: Optional[str]) -> Optional[str]:
    normalized = re.sub(r"[^0-9A-Za-z]", "", article or "").upper()
    return normalized or None


def _extract_article(text: str) -> Optional[str]:
    match = ARTICLE_RE.search(text or "")
    if match:
        return match.group(1)
    fallback = ARTICLE_FALLBACK_RE.search(text or "")
    return fallback.group(1) if fallback else None


def _product_id(url: str, title: str, flat: str) -> str:
    article = _normalize_article(_extract_article(f"{title} {flat}"))
    if article:
        return f"geberit-{article}"
    return f"geberit-{_stable_hash(_canonicalize_url(url))}"


def _iter_section_blocks(main) -> Iterable[Tuple[str, Any]]:
    for heading in main.select("h1, h2, h3, h4"):
        title = _clean_text(heading.get_text(" ", strip=True))
        if not title:
            continue
        container = heading.parent if getattr(heading, "parent", None) is not None else heading
        yield title, container


def _section_links(html: str, base_url: str, section_names: Sequence[str]) -> Set[str]:
    wanted = [_normalize_lookup(name) for name in section_names]
    main = _main_node(html)
    links: Set[str] = set()

    for section_title, container in _iter_section_blocks(main):
        title_lower = _normalize_lookup(section_title)
        if not any(name in title_lower for name in wanted):
            continue
        for anchor in container.select("a[href]"):
            href = anchor.get("href") or ""
            absolute = _canonicalize_url(urljoin(base_url, href))
            if _catalog_product_url(absolute):
                links.add(absolute)

    if links:
        return links

    for anchor in main.select("a[href]"):
        text = _normalize_lookup(_clean_text(anchor.get_text(" ", strip=True)))
        href = anchor.get("href") or ""
        absolute = _canonicalize_url(urljoin(base_url, href))
        if _catalog_product_url(absolute) and any(needle in text for needle in wanted):
            links.add(absolute)
    return links


def _marketing_models(html: str) -> Set[str]:
    flat = _main_flat_text(html).lower()
    discovered = {model for model in MARKETING_MODELS if model in flat}
    soup = BeautifulSoup(html or "", "lxml")
    for anchor in soup.select("a[href]"):
        text = _clean_text(anchor.get_text(" ", strip=True)).lower()
        for model in MARKETING_MODELS:
            if model in text:
                discovered.add(model)
    return discovered


def _is_accessory_text(text: str) -> bool:
    return bool(ACCESSORY_RE.search(_normalize_lookup(text or "")))


def _is_rohbauset_page(title: str, flat: str) -> bool:
    return bool(ROHBAU_RE.search(_normalize_lookup(f"{title} {flat}")))


def _is_cleanline_product_page(title: str, flat: str, url: str, marketing_models: Set[str]) -> bool:
    text = f"{title} {flat} {url}".lower()
    if not _catalog_product_url(url):
        return False
    if _is_accessory_text(text):
        return False
    if _is_rohbauset_page(title, flat):
        return False
    if not DRAIN_RE.search(_normalize_lookup(text)):
        return False
    if CLEANLINE_RE.search(text):
        return True
    return any(model in text for model in marketing_models)


def _length_info(text: str, target_mm: int) -> Tuple[Optional[int], str, Optional[str], bool]:
    for match in LEN_RANGE_CM_RE.finditer(text or ""):
        start_mm = int(match.group(1)) * 10
        end_mm = int(match.group(2)) * 10
        lo, hi = sorted((start_mm, end_mm))
        return None, "range", f"{lo}-{hi} mm", lo <= target_mm <= hi
    for match in LEN_RANGE_MM_RE.finditer(text or ""):
        lo, hi = sorted((int(match.group(1)), int(match.group(2))))
        return None, "range", f"{lo}-{hi} mm", lo <= target_mm <= hi

    fixed_lengths = [int(match.group(1)) for match in LEN_MM_RE.finditer(text or "") if 300 <= int(match.group(1)) <= 2400]
    if fixed_lengths:
        return max(fixed_lengths), "fixed", None, False
    return None, "unknown", None, False


def _extract_flow(flat: str) -> Tuple[List[float], Optional[Tuple[int, int]]]:
    values: List[float] = []
    first_span: Optional[Tuple[int, int]] = None
    for context in FLOW_CONTEXT_RE.finditer(flat or ""):
        window_start = max(0, context.start() - 120)
        window_end = min(len(flat), context.end() + 120)
        segment = flat[window_start:window_end]
        for match in FLOW_LPS_RE.finditer(segment):
            try:
                value = float(match.group(1).replace(",", "."))
            except Exception:
                continue
            if not 0.1 <= value <= 5.0:
                continue
            if value not in values:
                values.append(value)
            if first_span is None:
                first_span = (window_start + match.start(), window_start + match.end())
    return sorted(values), first_span


def _extract_dn(flat: str) -> Tuple[List[str], Optional[Tuple[int, int]]]:
    dns: List[str] = []
    first_span: Optional[Tuple[int, int]] = None
    for match in DN_PAIR_RE.finditer(flat or ""):
        for dn_value in (match.group(1), match.group(2)):
            dn = f"DN{dn_value}"
            if dn not in dns:
                dns.append(dn)
        if first_span is None:
            first_span = (match.start(), match.end())
    for match in DN_SINGLE_RE.finditer(flat or ""):
        dn = f"DN{match.group(1)}"
        if dn not in dns:
            dns.append(dn)
            if first_span is None:
                first_span = (match.start(), match.end())
    return dns, first_span


def _extract_height(flat: str) -> Tuple[Optional[int], Optional[int], Optional[Tuple[int, int]]]:
    for context in INSTALL_CONTEXT_RE.finditer(_normalize_lookup(flat or "")):
        source = _normalize_lookup(flat or "")
        window_start = max(0, context.start() - 120)
        window_end = min(len(source), context.end() + 120)
        window = source[window_start:window_end]
        if TRAP_SEAL_RE.search(window):
            continue
        range_match = HEIGHT_RANGE_RE.search(window)
        if range_match:
            lo, hi = sorted((int(range_match.group(1)), int(range_match.group(2))))
            if 20 <= lo <= 350 and 20 <= hi <= 350:
                return lo, hi, (window_start + range_match.start(), window_start + range_match.end())
        single_match = HEIGHT_SINGLE_RE.search(window)
        if single_match:
            value = int(single_match.group(1))
            if 20 <= value <= 350:
                return value, value, (window_start + single_match.start(), window_start + single_match.end())
    return None, None, None


def _page_payload(url: str) -> Optional[Dict[str, Any]]:
    status, final_url, html, error = _safe_get_text(_canonicalize_url(url))
    if status != 200 or not html:
        return None
    canonical_url = _canonicalize_url(final_url)
    if not _catalog_product_url(canonical_url):
        return None
    return {
        "url": canonical_url,
        "html": html,
        "title": _extract_title(html, canonical_url),
        "flat": _main_flat_text(html),
        "error": error,
    }


def _rohbau_links_from_product(html: str, base_url: str) -> List[str]:
    links = _section_links(html, base_url, ["Zusaetzlich zu bestellen", "Weitere Produkte"])
    out: List[str] = []
    for url in sorted(links):
        if url not in out:
            out.append(url)
    return out


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100):
    target = int(target_length_mm)
    tolerance = int(tolerance_mm)
    min_length = max(0, target - tolerance)
    max_length = target + tolerance

    debug: List[Dict[str, Any]] = []
    candidates: List[Dict[str, Any]] = []

    marketing_status, marketing_final, marketing_html, marketing_error = _safe_get_text(MARKETING_SEED)
    marketing_models = set(MARKETING_MODELS)
    if marketing_status == 200 and marketing_html:
        marketing_models = _marketing_models(marketing_html) or marketing_models
    debug.append({
        "site": "geberit",
        "seed_url": MARKETING_SEED,
        "status_code": marketing_status,
        "final_url": marketing_final,
        "error": marketing_error,
        "candidates_found": len(marketing_models),
        "method": "marketing_seed",
        "is_index": None,
    })

    seed_payload = _page_payload(CATALOG_SEED)
    if not seed_payload:
        debug.append({
            "site": "geberit",
            "seed_url": CATALOG_SEED,
            "status_code": None,
            "final_url": CATALOG_SEED,
            "error": "Catalog seed could not be fetched.",
            "candidates_found": 0,
            "method": "catalog_seed",
            "is_index": None,
        })
        return [], debug

    first_wave_urls = {seed_payload["url"]}
    first_wave_urls.update(_section_links(seed_payload["html"], seed_payload["url"], ["Weitere Produkte", "Zusaetzlich zu bestellen"]))

    page_map: Dict[str, Dict[str, Any]] = {}
    for url in sorted(first_wave_urls):
        payload = _page_payload(url)
        if payload:
            page_map[payload["url"]] = payload

    second_wave_urls: Set[str] = set()
    for payload in page_map.values():
        second_wave_urls.update(_section_links(payload["html"], payload["url"], ["Weitere Produkte", "Zusaetzlich zu bestellen"]))

    for url in sorted(second_wave_urls):
        payload = _page_payload(url)
        if payload:
            page_map[payload["url"]] = payload

    bom_urls: List[str] = []
    product_urls: List[str] = []
    unknown_length_count = 0

    for payload in sorted(page_map.values(), key=lambda item: item["url"]):
        title = payload["title"]
        flat = payload["flat"]
        url = payload["url"]
        text = f"{title} {flat}"

        if _is_rohbauset_page(title, flat):
            bom_urls.append(url)
            continue
        if not _is_cleanline_product_page(title, flat, url, marketing_models):
            continue

        length_mm, length_mode, range_text, in_range = _length_info(text, target)
        if length_mode == "fixed" and length_mm is not None and not (min_length <= length_mm <= max_length):
            continue
        if length_mode == "range" and not in_range:
            continue

        flow_values, _ = _extract_flow(flat)
        dn_values, _ = _extract_dn(flat)
        if not flow_values or not dn_values:
            continue

        candidates.append({
            "manufacturer": "geberit",
            "product_id": _product_id(url, title, flat),
            "product_family": "CleanLine",
            "product_name": title,
            "product_url": url,
            "sources": url,
            "candidate_type": "drain",
            "complete_system": "yes",
            "selected_length_mm": target,
            "length_mode": length_mode,
            "length_delta_mm": None if length_mm is None else (length_mm - target),
            "discovery_evidence": "Length (range)" if length_mode == "range" and range_text else None,
        })
        product_urls.append(url)
        if length_mode == "unknown":
            unknown_length_count += 1

        for rohbau_url in _rohbau_links_from_product(payload["html"], url):
            if rohbau_url not in bom_urls:
                bom_urls.append(rohbau_url)

    deduped: Dict[str, Dict[str, Any]] = {}
    for candidate in candidates:
        product_id = str(candidate.get("product_id") or "").strip()
        if product_id and product_id not in deduped:
            deduped[product_id] = candidate

    debug.append({
        "site": "geberit",
        "seed_url": CATALOG_SEED,
        "status_code": 200,
        "final_url": seed_payload["url"],
        "error": "" if deduped else "No accepted candidates.",
        "candidates_found": len(deduped),
        "method": "catalog_seed",
        "is_index": None,
        "products_count": len(deduped),
        "bom_options_count": len(dict.fromkeys(bom_urls)),
        "unknown_length_count": unknown_length_count,
        "sample_products_urls": json.dumps(product_urls[:10], ensure_ascii=False),
        "sample_bom_urls": json.dumps(list(dict.fromkeys(bom_urls))[:10], ensure_ascii=False),
    })

    return list(deduped.values()), debug


def extract_parameters(product_url: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
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

    payload = _page_payload(product_url)
    if not payload:
        result["evidence"].append(("HTML fetch", "status=None err=fetch_failed", _canonicalize_url(product_url)))
        return result

    url = payload["url"]
    flat = payload["flat"]
    title = payload["title"]
    normalized_flat = _normalize_lookup(flat)
    result["evidence"].append(("HTML fetch", "status=200 err=", url))

    material_match = re.search(r"\b(edelstahl|stahl|kunststoff|metall)\b", normalized_flat, re.IGNORECASE)
    if material_match:
        result["material_detail"] = material_match.group(1)
        result["material_v4a"] = "yes" if material_match.group(1).lower() == "edelstahl" else None
        result["evidence"].append(("Material", material_match.group(1), url))

    length_mm, length_mode, range_text, _ = _length_info(f"{title} {flat}", 1200)
    if length_mm is not None:
        result["resolved_length_mm"] = length_mm
        result["evidence"].append(("Length", f"{length_mm} mm", url))
    elif length_mode == "range" and range_text:
        result["evidence"].append(("Length (range)", range_text, url))

    flow_values, flow_span = _extract_flow(flat)
    if flow_values:
        result["flow_rate_lps"] = max(flow_values)
        result["flow_rate_lps_options"] = json.dumps(flow_values, ensure_ascii=False)
        result["flow_rate_unit"] = "l/s"
        result["flow_rate_status"] = "ok"
        if flow_span:
            result["flow_rate_raw_text"] = _snippet(flat, flow_span[0], flow_span[1])
            result["evidence"].append(("Flow rate", result["flow_rate_raw_text"], url))

    dn_values, dn_span = _extract_dn(flat)
    if dn_values:
        result["outlet_dn"] = "/".join(dn_values)
        result["outlet_dn_default"] = dn_values[0]
        result["outlet_dn_options_json"] = json.dumps(dn_values, ensure_ascii=False)
        if dn_span:
            result["evidence"].append(("Outlet DN", _snippet(flat, dn_span[0], dn_span[1]), url))

    height_min, height_max, height_span = _extract_height(flat)
    if height_min is not None and height_max is not None:
        result["height_adj_min_mm"] = height_min
        result["height_adj_max_mm"] = height_max
        if height_span:
            result["evidence"].append(("Installation height", _snippet(normalized_flat, height_span[0], height_span[1]), url))

    return result


def _parse_rohbauset_page(url: str) -> Optional[Dict[str, Any]]:
    payload = _page_payload(url)
    if not payload:
        return None

    title = payload["title"]
    flat = payload["flat"]
    normalized_flat = _normalize_lookup(flat)
    article = _extract_article(f"{title} {flat}")
    normalized_article = _normalize_article(article)
    dn_values, dn_span = _extract_dn(flat)
    height_min, height_max, height_span = _extract_height(flat)

    evidence: List[Tuple[str, str, str]] = []
    if article:
        evidence.append(("BOM Article", article, payload["url"]))
    if dn_span:
        evidence.append(("BOM Outlet DN", _snippet(flat, dn_span[0], dn_span[1]), payload["url"]))
    if height_span:
        evidence.append(("BOM Installation height", _snippet(normalized_flat, height_span[0], height_span[1]), payload["url"]))

    return {
        "bom_code": f"ROHBAU-{normalized_article}" if normalized_article else f"ROHBAU-{_stable_hash(payload['url'])}",
        "bom_name": title,
        "bom_url": payload["url"],
        "article_no": article,
        "outlet_dn": "/".join(dn_values) if dn_values else None,
        "height_adj_min_mm": height_min,
        "height_adj_max_mm": height_max,
        "is_default": "yes",
        "evidence_json": json.dumps(evidence, ensure_ascii=False) if evidence else None,
    }


def get_bom_options(product_url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    _ = params
    payload = _page_payload(product_url)
    if not payload:
        return []

    options: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for rohbau_url in _rohbau_links_from_product(payload["html"], payload["url"]):
        if rohbau_url in seen:
            continue
        seen.add(rohbau_url)
        parsed = _parse_rohbauset_page(rohbau_url)
        if parsed:
            options.append(parsed)
    return options
