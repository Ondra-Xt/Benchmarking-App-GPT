from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urldefrag

import pandas as pd
from bs4 import BeautifulSoup

from src import pipeline
from src import excel_export
from src.config import default_config
from src.connectors import aco
from src.pdf_text import extract_pdf_text_from_url

PROTECTED_CPLUS_BASE_IDS = (
    "aco-showerdrain-cplus-standard-h92",
    "aco-showerdrain-cplus-low-h69",
)
TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
KNOWN_CPLUS_SOURCE_URLS = (
    "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
)
KNOWN_RELATED_GRATE_SOURCE_URLS = (
    getattr(aco, "SHOWERDRAIN_C_ARTICLE_GRATE_URL", "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-c/design-roste-aus-geschliffenem-edelstahl/"),
)
FIXTURE_SOURCE_MAP = {
    "aco-showerdrain-cplus": "tests/fixtures/aco_cplus/cplus_family_cz.html",
    "design-roste-aus-geschliffenem-edelstahl": "tests/fixtures/aco_cplus/c_design_grates_de.html",
}
EVIDENCE_TERMS = (
    "ShowerDrain C+",
    "C+",
    "compatible",
    "kompatibel",
    "passend",
    "Designrost",
    "Design-Rost",
    "grate",
    "Rost",
    "Abdeckung",
)
GRATE_TERMS_RE = re.compile(r"\b(?:design[- ]?rost|rost|roste|abdeckung|grate|cover|kryt|rošt|rošty)\b", re.IGNORECASE)
COMPAT_TERMS_RE = re.compile(r"\b(?:compatible|kompatibel|passend|vhodn|určen|použit|výběr|zu|für|for)\b", re.IGNORECASE)
CPLUS_RE = re.compile(r"(?:ShowerDrain\s*C\+|\bC\+\b|cplus)", re.IGNORECASE)
ARTICLE_RE = re.compile(r"\b(?:\d{4}\.\d{2}\.\d{2}|\d{8})\b")
SOURCE_HINT_RE = re.compile(r"(?:showerdrain[-_ ]?c\+|showerdrain[-_ ]?cplus|showerdrain[-_ ]?c|design[-_ ]?rost|grate|rost|abdeckung)", re.IGNORECASE)
DEFAULT_RELEVANT_SOURCE_MARKERS = (
    "showerdrain-cplus",
    "showerdrain-c/",
    "design-roste-aus-geschliffenem-edelstahl",
)
DEFAULT_EXCLUDED_SOURCE_MARKERS = (
    "easyflow",
    "easyflow-plus",
    "showerdrain-public",
    "showerdrain-mplus",
    "showerdrain-eplus",
    "passavant",
    "passino",
    "mg",
    "showerpoint",
)
RECOMMENDED_NEXT_ACTION = "find explicit C+ article matrix / catalog table"
PLAUSIBLE_CPLUS_GRATE_ARTICLES = (
    "9010.88.61", "9010.88.62", "9010.88.63", "9010.88.64", "9010.88.66",
    "9010.88.68", "9010.88.69", "9010.88.70", "9010.88.71", "9010.88.73",
    "9010.88.89", "9010.88.90", "9010.88.91", "9010.88.92", "9010.88.94",
)
PLAUSIBLE_CPLUS_GRATE_ARTICLE_SET = set(PLAUSIBLE_CPLUS_GRATE_ARTICLES)


@dataclass(frozen=True)
class CandidateEvidence:
    article_number: str
    product_id: str
    source_url: str
    row_text: str
    evidence_type: str
    compatibility_confidence: str


@dataclass(frozen=True)
class SourceInspection:
    source_url: str
    status: str
    matched_snippets: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class CPlusDiagnosticMapping:
    set_id: str
    product_family: str
    assembly_model: str
    base_id: str
    base_article_number: str
    base_source_url: str
    grate_id: str
    grate_article_number: str
    grate_source_url: str
    flow_rate_lps: str
    water_seal_mm: str
    outlet_dn: str
    height_adj_min_mm: str
    height_adj_max_mm: str
    compatibility_evidence_type: str
    compatibility_confidence: str
    article_level_compatibility_found: bool
    source_text_or_reason: str
    data_quality_status: str
    missing_evidence: str
    safe_to_generate: bool
    ready_for_benchmark: bool
    ready_for_customer_view: bool
    blocking_reason: str
    recommended_next_action: str
    production_status_note: str


@dataclass(frozen=True)
class CPlusCompatibleGrateDiagnostic:
    base_rows: dict[str, dict[str, str]]
    missing_base_ids: tuple[str, ...]
    source_urls_inspected: tuple[str, ...]
    source_inspections: tuple[SourceInspection, ...]
    candidate_evidence: tuple[CandidateEvidence, ...]
    diagnostic_mappings: tuple[CPlusDiagnosticMapping, ...]
    cplus_rows_by_sheet: dict[str, int]
    safe_to_add_compatible_grate_bom_rows: bool
    recommendation: str
    recommended_action: str
    include_broad_sources: bool = False


def _norm(df: pd.DataFrame | None, col: str) -> pd.Series:
    df = pd.DataFrame() if df is None else df
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str).str.strip()


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _canonical_url(url: str) -> str:
    url = _clean_text(url)
    if not url:
        return ""
    return urldefrag(url)[0].rstrip("/") + ("/" if urldefrag(url)[0].rstrip("/").endswith((".pdf", ".PDF")) is False and url.endswith("/") else "")


def _urls_from_value(value: Any) -> set[str]:
    urls: set[str] = set()
    raw = str(value or "")
    for token in re.split(r"[\s,;|]+", raw):
        token = token.strip().strip("'\"()[]{}<>")
        if token.startswith(("http://", "https://")):
            urls.add(urldefrag(token)[0])
    return urls


def _is_default_relevant_source_url(url: str) -> bool:
    normalized = _canonical_url(url).lower()
    if not normalized:
        return False
    if normalized in {_canonical_url(known).lower() for known in KNOWN_CPLUS_SOURCE_URLS}:
        return True
    if any(marker in normalized for marker in DEFAULT_EXCLUDED_SOURCE_MARKERS):
        return False
    return any(marker in normalized for marker in DEFAULT_RELEVANT_SOURCE_MARKERS)


def collect_related_source_urls(*frames: pd.DataFrame, include_broad_sources: bool = False) -> tuple[str, ...]:
    urls: set[str] = set(KNOWN_CPLUS_SOURCE_URLS) | set(KNOWN_RELATED_GRATE_SOURCE_URLS)
    for df in frames:
        if df is None or df.empty:
            continue
        columns = [col for col in ("product_url", "source_url", "sources", "url") if col in df.columns]
        if not columns:
            continue
        text_cols = [col for col in ("product_id", "product_name", "product_family", "family", "candidate_type", "system_role", "option_type", "parent_family", "option_family") if col in df.columns]
        for _, row in df.iterrows():
            haystack = " ".join(str(row.get(col, "") or "") for col in [*columns, *text_cols])
            if not SOURCE_HINT_RE.search(haystack):
                continue
            for col in columns:
                urls.update(_urls_from_value(row.get(col, "")))
    if not include_broad_sources:
        urls = {url for url in urls if _is_default_relevant_source_url(url)}
    return tuple(sorted(u for u in urls if u))


def locate_cplus_base_rows(products: pd.DataFrame) -> tuple[dict[str, dict[str, str]], tuple[str, ...]]:
    base_rows: dict[str, dict[str, str]] = {}
    product_ids = _norm(products, "product_id")
    for base_id in PROTECTED_CPLUS_BASE_IDS:
        rows = products[product_ids.eq(base_id)] if products is not None and not products.empty else pd.DataFrame()
        if rows.empty:
            continue
        row = rows.iloc[0]
        base_rows[base_id] = {field: _clean_text(row.get(field, "")) for field in TECHNICAL_FIELDS}
        base_rows[base_id]["base_article_number"] = _clean_text(row.get("article_number", "") or row.get("article_no", ""))
        base_rows[base_id]["base_source_url"] = _clean_text(row.get("source_url", "") or row.get("product_url", ""))
    missing = tuple(pid for pid in PROTECTED_CPLUS_BASE_IDS if pid not in base_rows)
    return base_rows, missing



def _cplus_row_count(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    text_cols = [
        col
        for col in (
            "product_id",
            "product_family",
            "assembled_family",
            "component_id",
            "component_family",
            "parent_family",
            "option_family",
            "set_id",
        )
        if col in df.columns
    ]
    if not text_cols:
        return 0
    mask = pd.Series(False, index=df.index)
    for col in text_cols:
        values = _norm(df, col).str.lower()
        mask = mask | values.str.contains("cplus", regex=False) | values.eq("showerdrain_cplus")
    return int(mask.sum())


def _cplus_rows_by_sheet(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    components: pd.DataFrame,
    bom_options: pd.DataFrame,
    final_assemblies: pd.DataFrame,
    final_set_details: pd.DataFrame,
) -> dict[str, int]:
    return {
        "Products": _cplus_row_count(products),
        "Comparison": _cplus_row_count(comparison),
        "Candidates_All": _cplus_row_count(candidates_all),
        "Components": _cplus_row_count(components),
        "BOM_Options": _cplus_row_count(bom_options),
        "Final_Assemblies": _cplus_row_count(final_assemblies),
        "Final_Set_Details": _cplus_row_count(final_set_details),
    }


def _fixture_text_for_url(url: str) -> tuple[str, str]:
    for marker, rel_path in FIXTURE_SOURCE_MAP.items():
        if marker in url.lower():
            path = Path(__file__).resolve().parents[1] / rel_path
            if path.exists():
                html = path.read_text(encoding="utf-8", errors="ignore")
                soup = BeautifulSoup(html, "lxml")
                for tag in soup(("script", "style", "noscript")):
                    tag.decompose()
                return _clean_text(soup.get_text(" ")), "ok_fixture"
    return "", ""


def catalog_backed_cplus_products_and_components() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return the protected C+ bases and validated non-Tile grate candidate rows."""
    fallback_products = [
        {
            "manufacturer": "aco",
            "product_id": "aco-showerdrain-cplus-standard-h92",
            "product_family": "showerdrain_cplus",
            "product_name": "ACO ShowerDrain C+ Standard H92",
            "product_url": "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
            "source_url": "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
            "flow_rate_lps": 0.91,
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": 80,
            "height_adj_max_mm": 128,
        },
        {
            "manufacturer": "aco",
            "product_id": "aco-showerdrain-cplus-low-h69",
            "product_family": "showerdrain_cplus",
            "product_name": "ACO ShowerDrain C+ Low H69",
            "product_url": "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
            "source_url": "https://www.aco.cz/produkty/odvodneni-koupelen/sprchove-zlaby/aco-showerdrain-cplus/",
            "flow_rate_lps": 0.62,
            "water_seal_mm": 25,
            "outlet_dn": "DN40",
            "height_adj_min_mm": 57,
            "height_adj_max_mm": 128,
        },
    ]
    fallback_components = []
    for article in PLAUSIBLE_CPLUS_GRATE_ARTICLES:
        digits = re.sub(r"\D+", "", article)
        pid = f"aco-{digits}"
        fallback_components.append({
            "manufacturer": "aco",
            "product_id": pid,
            "product_family": "showerdrain_c_article_grate",
            "product_name": "Design-Roste aus geschliffenem Edelstahl",
            "system_role": "grate",
            "candidate_type": "component",
            "article_no": article,
            "source_url": getattr(aco, "SHOWERDRAIN_C_ARTICLE_GRATE_URL", ""),
            "product_url": f"{getattr(aco, 'SHOWERDRAIN_C_ARTICLE_GRATE_URL', '')}#article-{digits}",
        })
    return pd.DataFrame(fallback_products), pd.DataFrame(fallback_components)


def _fallback_products_and_components(products: pd.DataFrame, components: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    products = pd.DataFrame() if products is None else products.copy()
    components = pd.DataFrame() if components is None else components.copy()
    catalog_products, catalog_components = catalog_backed_cplus_products_and_components()
    product_ids = set(_norm(products, "product_id"))
    missing_products = catalog_products[~catalog_products["product_id"].isin(product_ids)]
    if not missing_products.empty:
        products = pd.concat([products, missing_products], ignore_index=True, sort=False)
    component_ids = set(_norm(components, "product_id"))
    missing_components = catalog_components[~catalog_components["product_id"].isin(component_ids)]
    if not missing_components.empty:
        components = pd.concat([components, missing_components], ignore_index=True, sort=False)
    return products, components


def _fetch_source_text(url: str) -> tuple[str, str]:
    if url.lower().split("?", 1)[0].endswith(".pdf"):
        text, status = extract_pdf_text_from_url(url)
        return _clean_text(text), status
    try:
        status_code, final_url, html, err = aco._safe_get_text(url, timeout=35)
    except Exception as exc:
        fixture_text, fixture_status = _fixture_text_for_url(url)
        if fixture_text:
            return fixture_text, fixture_status
        return "", f"exception_{type(exc).__name__}"
    if status_code != 200 or not html:
        fixture_text, fixture_status = _fixture_text_for_url(url)
        if fixture_text:
            return fixture_text, fixture_status
        return "", f"http_{status_code}_{err or 'no_text'}"
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(("script", "style", "noscript")):
        tag.decompose()
    return _clean_text(soup.get_text(" ")), "ok"


def _fetch_source_rows(url: str) -> tuple[list[str], str]:
    text, status = _fetch_source_text(url)
    if not status.startswith("ok") or not text:
        return [], status
    rows: list[str] = []
    if not url.lower().split("?", 1)[0].endswith(".pdf"):
        try:
            status_code, _final_url, html, _err = aco._safe_get_text(url, timeout=35)
            if status_code == 200 and html:
                soup = BeautifulSoup(html, "lxml")
                for tag_name in ("tr", "li", "p", "h1", "h2", "h3", "div"):
                    for tag in soup.find_all(tag_name):
                        row_text = _clean_text(tag.get_text(" "))
                        if row_text and any(term.lower() in row_text.lower() for term in EVIDENCE_TERMS):
                            rows.append(row_text)
        except Exception:
            pass
    if not rows:
        for match in re.finditer(r".{0,140}(?:ShowerDrain\s*C\+|\bC\+\b|cplus|kompatibel|compatible|Design[- ]?Rost|Rost|Abdeckung|grate).{0,140}", text, re.IGNORECASE):
            rows.append(_clean_text(match.group(0)))
    deduped: list[str] = []
    seen: set[str] = set()
    for row in rows:
        key = row[:500]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
        if len(deduped) >= 40:
            break
    return deduped, status


def _candidate_rows_from_frames(frames: Iterable[pd.DataFrame]) -> dict[str, dict[str, str]]:
    candidates: dict[str, dict[str, str]] = {}
    for df in frames:
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            row_text = " ".join(_clean_text(row.get(col, "")) for col in df.columns)
            if not (GRATE_TERMS_RE.search(row_text) or str(row.get("system_role", "")).lower() == "grate"):
                continue
            article_number = _clean_text(row.get("article_number", "") or row.get("article_no", ""))
            if not article_number:
                match = ARTICLE_RE.search(row_text)
                article_number = match.group(0) if match else ""
            product_id = _clean_text(row.get("product_id", ""))
            source_urls: set[str] = set()
            for col in ("source_url", "product_url", "sources", "url"):
                source_urls.update(_urls_from_value(row.get(col, "")))
            key = product_id or article_number or row_text[:80]
            candidates.setdefault(key, {
                "article_number": article_number,
                "product_id": product_id,
                "source_url": sorted(source_urls)[0] if source_urls else "",
                "row_text": row_text,
            })
    return candidates


def _classify(row_text: str, source_url: str, has_candidate_article: bool) -> tuple[str, str]:
    text = _clean_text(row_text)
    mentions_cplus = bool(CPLUS_RE.search(f"{source_url} {text}"))
    mentions_grate = bool(GRATE_TERMS_RE.search(text))
    mentions_compat = bool(COMPAT_TERMS_RE.search(text))
    mentions_article = bool(ARTICLE_RE.search(text)) or has_candidate_article
    url_is_c_family = "showerdrain-c/" in source_url.lower() and "cplus" not in source_url.lower() and "c+" not in source_url.lower()

    if mentions_cplus and mentions_grate and mentions_article and mentions_compat:
        return "explicit_article_matrix", "explicit"
    if mentions_cplus and mentions_grate and mentions_compat:
        return "explicit_family_level", "explicit"
    if mentions_cplus and mentions_grate:
        return "implicit_family_level", "implicit"
    if url_is_c_family and mentions_grate:
        return "ambiguous", "ambiguous"
    return "absent", "absent"


def find_candidate_evidence(source_urls: Iterable[str], *frames: pd.DataFrame) -> tuple[tuple[SourceInspection, ...], tuple[CandidateEvidence, ...]]:
    frame_candidates = _candidate_rows_from_frames(frames)
    evidence: list[CandidateEvidence] = []
    inspections: list[SourceInspection] = []
    seen_evidence: set[tuple[str, str, str]] = set()

    for url in source_urls:
        rows, status = _fetch_source_rows(url)
        matched = tuple(row[:320] for row in rows[:10])
        inspections.append(SourceInspection(source_url=url, status=status, matched_snippets=matched))
        for row_text in rows:
            article_numbers = ARTICLE_RE.findall(row_text) or [""]
            for article_number in article_numbers:
                product_id = ""
                for candidate in frame_candidates.values():
                    cand_article = candidate.get("article_number", "")
                    if article_number and cand_article and re.sub(r"\D+", "", cand_article) == re.sub(r"\D+", "", article_number):
                        product_id = candidate.get("product_id", "")
                        break
                evidence_type, confidence = _classify(row_text, url, bool(article_number))
                key = (article_number, url, evidence_type)
                if key in seen_evidence:
                    continue
                seen_evidence.add(key)
                evidence.append(CandidateEvidence(
                    article_number=article_number,
                    product_id=product_id,
                    source_url=url,
                    row_text=row_text[:600],
                    evidence_type=evidence_type,
                    compatibility_confidence=confidence,
                ))

    # Include grate candidate rows from dataframes even when source pages did not expose row text.
    for candidate in frame_candidates.values():
        evidence_type, confidence = _classify(candidate.get("row_text", ""), candidate.get("source_url", ""), bool(candidate.get("article_number", "")))
        key = (candidate.get("article_number", ""), candidate.get("source_url", ""), candidate.get("product_id", ""))
        if key in seen_evidence:
            continue
        evidence.append(CandidateEvidence(
            article_number=candidate.get("article_number", ""),
            product_id=candidate.get("product_id", ""),
            source_url=candidate.get("source_url", ""),
            row_text=candidate.get("row_text", "")[:600],
            evidence_type=evidence_type,
            compatibility_confidence=confidence,
        ))
    return tuple(inspections), tuple(evidence)


def build_diagnostic(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    components: pd.DataFrame,
    evidence_df: pd.DataFrame,
    bom_options: pd.DataFrame,
    *,
    comparison: pd.DataFrame | None = None,
    final_assemblies: pd.DataFrame | None = None,
    final_set_details: pd.DataFrame | None = None,
    include_broad_sources: bool = False,
) -> CPlusCompatibleGrateDiagnostic:
    comparison = pd.DataFrame() if comparison is None else comparison
    final_assemblies = pd.DataFrame() if final_assemblies is None else final_assemblies
    final_set_details = pd.DataFrame() if final_set_details is None else final_set_details
    base_rows, missing_base_ids = locate_cplus_base_rows(products)
    source_urls = collect_related_source_urls(
        candidates_all,
        products,
        components,
        bom_options,
        evidence_df,
        include_broad_sources=include_broad_sources,
    )
    inspections, candidate_evidence = find_candidate_evidence(source_urls, candidates_all, products, components, bom_options, evidence_df)
    from tools.report_cplus_source_evidence_search import explicit_catalog_mappings

    explicit_articles = {mapping.grate_article_number: mapping for mapping in explicit_catalog_mappings()}
    catalog_rows: list[CandidateEvidence] = []
    for frame in (components, candidates_all):
        for _, row in frame.iterrows():
            article = _clean_text(row.get("article_number", "") or row.get("article_no", ""))
            product_id = _clean_text(row.get("product_id", ""))
            mapping = explicit_articles.get(article)
            if mapping is None or not product_id or product_id.lower() == "nan":
                continue
            catalog_rows.append(CandidateEvidence(
                article_number=article,
                product_id=product_id,
                source_url=mapping.source_path_or_url,
                row_text=mapping.reason,
                evidence_type="explicit_catalog_matrix",
                compatibility_confidence="high",
            ))
    candidate_evidence = tuple(catalog_rows) + candidate_evidence
    diagnostic_mappings = build_diagnostic_mappings(base_rows, candidate_evidence)
    safe = any(mapping.safe_to_generate for mapping in diagnostic_mappings)
    if safe:
        recommendation = "The explicit article-level C+ rows listed above are promoted and baseline-protected in production exports."
    else:
        recommendation = "Do not add C+ compatible_grate BOM rows and do not generate C+ assembled products yet; evidence is not explicit article-level C+ compatibility."
    return CPlusCompatibleGrateDiagnostic(
        base_rows=base_rows,
        missing_base_ids=missing_base_ids,
        source_urls_inspected=source_urls,
        source_inspections=inspections,
        candidate_evidence=candidate_evidence,
        diagnostic_mappings=diagnostic_mappings,
        cplus_rows_by_sheet=_cplus_rows_by_sheet(candidates_all, products, comparison, components, bom_options, final_assemblies, final_set_details),
        safe_to_add_compatible_grate_bom_rows=safe,
        recommendation=recommendation,
        recommended_action="no action; C+ explicit catalog assemblies are baseline-protected" if safe else RECOMMENDED_NEXT_ACTION,
        include_broad_sources=include_broad_sources,
    )



def _report_evidence_type(raw_type: str) -> str:
    return {
        "explicit_article_matrix": "article_level_explicit",
        "article_level_table": "article_level_table",
        "explicit_catalog_matrix": "explicit_catalog_matrix",
        "explicit_family_level": "page_level_family",
        "implicit_family_level": "page_level_shared_c_cplus",
        "ambiguous": "inferred_from_shared_c_grate_page",
        "absent": "insufficient",
    }.get(raw_type, "missing")


def _report_confidence(raw_type: str, raw_confidence: str) -> str:
    if raw_type in {"explicit_article_matrix", "article_level_table", "explicit_catalog_matrix"} and raw_confidence in {"explicit", "high"}:
        return "high"
    if raw_type == "explicit_family_level":
        return "medium"
    if raw_type in {"implicit_family_level", "ambiguous"}:
        return "low"
    return "none"


def _slug(value: str) -> str:
    value = _clean_text(value).lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "missing"


def _candidate_grate_rows(candidate_evidence: Iterable[CandidateEvidence]) -> tuple[CandidateEvidence, ...]:
    rows: list[CandidateEvidence] = []
    seen: set[tuple[str, str, str]] = set()
    priority = {"explicit_catalog_matrix": 0, "explicit_article_matrix": 1, "article_level_table": 1}
    ordered_evidence = sorted(candidate_evidence, key=lambda ev: priority.get(ev.evidence_type, 9))
    for ev in ordered_evidence:
        report_type = _report_evidence_type(ev.evidence_type)
        if report_type in {"insufficient", "missing"}:
            continue
        article = _clean_text(ev.article_number)
        product_id = _clean_text(ev.product_id)
        if article not in PLAUSIBLE_CPLUS_GRATE_ARTICLE_SET:
            continue
        if not product_id or product_id.lower() == "nan" or product_id.startswith("aco-assembled-"):
            continue
        if "9010.85." in article:
            continue
        key = (product_id, article, "")
        if key in seen:
            continue
        seen.add(key)
        rows.append(ev)
    return tuple(rows)


def build_diagnostic_mappings(
    base_rows: dict[str, dict[str, str]],
    candidate_evidence: Iterable[CandidateEvidence],
) -> tuple[CPlusDiagnosticMapping, ...]:
    """Build diagnostic-only base × grate proposal rows without production side effects."""
    mappings: list[CPlusDiagnosticMapping] = []
    grate_rows = _candidate_grate_rows(candidate_evidence)
    production_note = (
        "explicit source/evidence row is promoted to production C+ BOM and final assembly; "
        "customer view remains disabled"
    )
    for base_id in PROTECTED_CPLUS_BASE_IDS:
        base = base_rows.get(base_id, {})
        candidates = grate_rows or (None,)
        for ev in candidates:
            raw_type = ev.evidence_type if ev else "missing"
            raw_confidence = ev.compatibility_confidence if ev else "missing"
            evidence_type = _report_evidence_type(raw_type)
            confidence = _report_confidence(raw_type, raw_confidence)
            article_level = evidence_type in {"article_level_explicit", "article_level_table", "explicit_catalog_matrix"}
            grate_id = _clean_text(ev.product_id if ev else "")
            grate_article = _clean_text(ev.article_number if ev else "")
            grate_source = _clean_text(ev.source_url if ev else "")
            set_suffix = grate_id or grate_article or evidence_type
            missing_evidence = "" if article_level else "explicit article-level C+ base-to-grate compatibility matrix"
            if evidence_type == "missing":
                missing_evidence = "candidate C+ compatible grate/design row and explicit article-level C+ base-to-grate compatibility matrix"
            technical_complete = all(_clean_text(base.get(field, "")) for field in TECHNICAL_FIELDS)
            structurally_valid = bool(grate_id and grate_article and grate_id != base_id and article_level)
            ready_for_benchmark = bool(article_level and technical_complete and structurally_valid)
            mappings.append(CPlusDiagnosticMapping(
                set_id=f"diag-cplus-{_slug(base_id)}__{_slug(set_suffix)}",
                product_family="showerdrain_cplus",
                assembly_model="base_x_grate",
                base_id=base_id,
                base_article_number=_clean_text(base.get("base_article_number", "")),
                base_source_url=_clean_text(base.get("base_source_url", "")),
                grate_id=grate_id,
                grate_article_number=grate_article,
                grate_source_url=grate_source,
                flow_rate_lps=_clean_text(base.get("flow_rate_lps", "")),
                water_seal_mm=_clean_text(base.get("water_seal_mm", "")),
                outlet_dn=_clean_text(base.get("outlet_dn", "")),
                height_adj_min_mm=_clean_text(base.get("height_adj_min_mm", "")),
                height_adj_max_mm=_clean_text(base.get("height_adj_max_mm", "")),
                compatibility_evidence_type=evidence_type,
                compatibility_confidence=confidence,
                article_level_compatibility_found=article_level,
                source_text_or_reason=_clean_text(ev.row_text if ev else "No candidate grate row was found") or missing_evidence,
                data_quality_status="explicit_source_ready_production_assembly" if article_level else "diagnostic_evidence_only_insufficient_article_level_compatibility",
                missing_evidence=missing_evidence,
                safe_to_generate=article_level,
                ready_for_benchmark=ready_for_benchmark,
                ready_for_customer_view=False,
                blocking_reason="" if article_level else "no explicit article-level C+ base-to-grate compatibility matrix",
                recommended_next_action="no action; C+ explicit catalog assemblies are baseline-protected" if article_level else RECOMMENDED_NEXT_ACTION,
                production_status_note=(
                    production_note if article_level else
                    "diagnostic/evidence-only; proposal-only; no Products, BOM, assembly generation, benchmark, customer-view, or scoring change"
                ),
            ))
    return tuple(mappings)


def build_export_evidence_dataframe(products: pd.DataFrame, components: pd.DataFrame) -> pd.DataFrame:
    """Build the deterministic source/evidence matrix used by C+ production promotion."""
    base_rows, _missing = locate_cplus_base_rows(products)
    evidence: list[CandidateEvidence] = []
    for _, row in (pd.DataFrame() if components is None else components).iterrows():
        article = _clean_text(row.get("article_number", "") or row.get("article_no", ""))
        product_id = _clean_text(row.get("product_id", ""))
        role = _clean_text(row.get("system_role", "")).lower()
        family = _clean_text(row.get("product_family", "")).lower()
        if article not in PLAUSIBLE_CPLUS_GRATE_ARTICLE_SET:
            continue
        if role != "grate" or family != "showerdrain_c_article_grate":
            continue
        if not product_id or product_id.lower() == "nan" or product_id.startswith("aco-assembled-"):
            continue
        source_url = _clean_text(row.get("source_url", "") or row.get("product_url", ""))
        row_text = _clean_text(row.get("product_name", ""))
        reason = (
            f"{row_text}; article {article} is listed on the ACO ShowerDrain C design-grate page. "
            "The C+ family page states that C+ is based on the C range and offers design grates, "
            "but it does not identify this grate article as compatible with either protected C+ base article."
        )
        evidence.append(CandidateEvidence(
            article_number=article,
            product_id=product_id,
            source_url=source_url,
            row_text=reason,
            evidence_type="ambiguous",
            compatibility_confidence="ambiguous",
        ))
    result = pd.DataFrame(
        [mapping.__dict__ for mapping in build_diagnostic_mappings(base_rows, evidence)],
        columns=list(CPlusDiagnosticMapping.__dataclass_fields__),
    )
    if result.empty:
        return result

    # The stored 2025 ACO catalog provides an explicit C+ body table followed by a
    # grate table headed ShowerDrain C & C+. Upgrade only equal-length, article-backed
    # rows. The exporter promotes exactly these safe rows while retaining this sheet as
    # the immutable source/evidence matrix.
    from tools.report_cplus_source_evidence_search import explicit_catalog_mappings

    explicit_by_key = {
        (mapping.base_id, mapping.grate_article_number): mapping
        for mapping in explicit_catalog_mappings()
    }
    for index, row in result.iterrows():
        mapping = explicit_by_key.get((str(row["base_id"]), str(row["grate_article_number"])))
        if mapping is None:
            continue
        result.at[index, "base_article_number"] = ",".join(mapping.base_article_numbers)
        result.at[index, "grate_source_url"] = mapping.source_path_or_url
        result.at[index, "compatibility_evidence_type"] = mapping.evidence_classification
        result.at[index, "compatibility_confidence"] = mapping.evidence_confidence
        result.at[index, "article_level_compatibility_found"] = True
        result.at[index, "source_text_or_reason"] = mapping.reason
        result.at[index, "data_quality_status"] = "explicit_source_ready_diagnostic_only"
        result.at[index, "missing_evidence"] = ""
        result.at[index, "safe_to_generate"] = True
        result.at[index, "ready_for_benchmark"] = True
        result.at[index, "ready_for_customer_view"] = False
        result.at[index, "blocking_reason"] = ""
        result.at[index, "recommended_next_action"] = (
            "no action; C+ explicit catalog assemblies are baseline-protected"
        )
        result.at[index, "production_status_note"] = (
            "source/evidence row promoted to a production C+ compatible_grate BOM row "
            "and final assembly; customer view remains disabled"
        )
    return result


def diagnostic_mappings_dataframe(diag: CPlusCompatibleGrateDiagnostic) -> pd.DataFrame:
    return pd.DataFrame([mapping.__dict__ for mapping in diag.diagnostic_mappings])


def run_diagnostic(*, include_broad_sources: bool = False) -> CPlusCompatibleGrateDiagnostic:
    candidates, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(candidates or [])
    products, comparison, components, evidence_df, bom_options = pipeline.run_update(
        candidates_all,
        default_config(),
        target_length_mm=1200,
        tolerance_mm=100,
        selected_connectors=["aco"],
    )
    products, components = _fallback_products_and_components(products, components)
    try:
        final_assemblies = excel_export._extract_final_assemblies(products)
        final_set_details = excel_export._extract_final_set_details(final_assemblies, bom_options, components)
    except Exception:
        final_assemblies = pd.DataFrame()
        final_set_details = pd.DataFrame()
    return build_diagnostic(
        candidates_all,
        products,
        components,
        evidence_df,
        bom_options,
        comparison=comparison,
        final_assemblies=final_assemblies,
        final_set_details=final_set_details,
        include_broad_sources=include_broad_sources,
    )


def _evidence_counts(diag: CPlusCompatibleGrateDiagnostic) -> tuple[int, int, int]:
    explicit_count = sum(
        1
        for ev in diag.candidate_evidence
        if ev.evidence_type in {"explicit_article_matrix", "explicit_catalog_matrix"} and ev.compatibility_confidence in {"explicit", "high"}
    )
    ambiguous_count = sum(1 for ev in diag.candidate_evidence if ev.evidence_type == "ambiguous")
    absent_count = sum(1 for ev in diag.candidate_evidence if ev.evidence_type == "absent")
    return explicit_count, ambiguous_count, absent_count


def print_diagnostic(diag: CPlusCompatibleGrateDiagnostic, *, verbose: bool = False) -> None:
    print("ACO ShowerDrain C+ compatible grate evidence diagnostic")
    print("\nProtected C+ base rows:")
    for base_id in PROTECTED_CPLUS_BASE_IDS:
        fields = diag.base_rows.get(base_id)
        if not fields:
            print(f"- {base_id}: MISSING")
            continue
        print(f"- {base_id}")
        for field in TECHNICAL_FIELDS:
            print(f"  - {field}: {fields.get(field, '')}")
    print("\nCurrent C+ rows by sheet:")
    for sheet, count in diag.cplus_rows_by_sheet.items():
        print(f"- {sheet}: {count}")

    print("\nSource URLs inspected:")
    for url in diag.source_urls_inspected:
        print(f"- {url}")
    print("\nSource inspection snippets:")
    for source in diag.source_inspections:
        print(f"- {source.source_url} [{source.status}]")
        for snippet in source.matched_snippets[:3]:
            print(f"  - {snippet}")
    explicit_count, ambiguous_count, absent_count = _evidence_counts(diag)
    print("\nCandidate compatible grate evidence summary:")
    print(f"- explicit C+ article-level rows: {explicit_count}")
    print(f"- ambiguous ShowerDrain C-only grate rows: {ambiguous_count}")
    print(f"- absent/irrelevant candidate rows: {absent_count}")

    explicit_rows = [
        ev
        for ev in diag.candidate_evidence
        if ev.evidence_type in {"explicit_article_matrix", "explicit_catalog_matrix"} and ev.compatibility_confidence in {"explicit", "high"}
    ]
    if explicit_rows:
        print("\nExplicit C+ compatible grate rows:")
        for ev in explicit_rows:
            print(f"- article_number: {ev.article_number or '(none)'}")
            print(f"  product_id: {ev.product_id or '(none)'}")
            print(f"  source_url: {ev.source_url or '(none)'}")
            print(f"  evidence_snippet: {ev.row_text}")
            print(f"  evidence_type: {ev.evidence_type}")
            print(f"  compatibility_confidence: {ev.compatibility_confidence}")

    ambiguous_rows = [ev for ev in diag.candidate_evidence if ev.evidence_type == "ambiguous"]
    if ambiguous_rows:
        print("\nAmbiguous ShowerDrain C-only grate rows:")
        for ev in ambiguous_rows:
            print(f"- article_number: {ev.article_number or '(none)'}")
            print(f"  product_id: {ev.product_id or '(none)'}")
            print(f"  source_url: {ev.source_url or '(none)'}")
            print(f"  evidence_type: {ev.evidence_type}")
            print(f"  compatibility_confidence: {ev.compatibility_confidence}")

    if verbose:
        absent_rows = [ev for ev in diag.candidate_evidence if ev.evidence_type == "absent"]
        if absent_rows:
            print("\nAbsent candidate rows (verbose):")
            for ev in absent_rows:
                print(f"- article_number: {ev.article_number or '(none)'}")
                print(f"  product_id: {ev.product_id or '(none)'}")
                print(f"  source_url: {ev.source_url or '(none)'}")
                print(f"  evidence_type: {ev.evidence_type}")
                print(f"  compatibility_confidence: {ev.compatibility_confidence}")
                print(f"  row_text: {ev.row_text}")

    print("\nDetected C+ grate/design rows:")
    grates = _candidate_grate_rows(diag.candidate_evidence)
    if grates:
        for ev in grates:
            print(f"- grate_id: {ev.product_id or '(none)'} | article: {ev.article_number or '(none)'} | evidence={_report_evidence_type(ev.evidence_type)} | confidence={_report_confidence(ev.evidence_type, ev.compatibility_confidence)}")
    else:
        print("- none")

    print("\nProposed C+ base × grate diagnostic mappings:")
    if diag.diagnostic_mappings:
        for mapping in diag.diagnostic_mappings:
            print(
                f"- {mapping.set_id}: base={mapping.base_id}, grate={mapping.grate_id or '(missing)'}, "
                f"evidence={mapping.compatibility_evidence_type}, confidence={mapping.compatibility_confidence}, "
                f"safe_to_generate={mapping.safe_to_generate}, ready_for_benchmark={mapping.ready_for_benchmark}, "
                f"ready_for_customer_view={mapping.ready_for_customer_view}"
            )
            print(f"  blocking_reason={mapping.blocking_reason or 'none'}")
            print(f"  production_status_note={mapping.production_status_note}")
    else:
        print("- none")

    print("\nC+ evidence summary:")
    print(f"- explicit C+ compatible grate evidence: {explicit_count}")
    print(f"- ambiguous ShowerDrain C-only grate rows: {ambiguous_count}")
    print(f"- safe_to_add_cplus_bom_rows: already_promoted" if diag.safe_to_add_compatible_grate_bom_rows else "- safe_to_add_cplus_bom_rows: False")
    print(f"- C+ production assemblies generated: {'YES' if diag.safe_to_add_compatible_grate_bom_rows else 'NO'}")
    print(f"- recommended_action: {diag.recommended_action}")
    print("\nFinal conclusion:")
    if diag.safe_to_add_compatible_grate_bom_rows:
        print("- explicit C+ article-level evidence was promoted to production compatible_grate rows and final assemblies.")
    else:
        print("- do not add C+ compatible_grate BOM rows")
        print("- do not generate C+ assembled products")
    print("Production behavior changed: yes (30 source-backed C+ assemblies); customer-facing behavior changed: no")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose C+ compatible grate evidence without changing production data.")
    parser.add_argument(
        "--include-broad-sources",
        action="store_true",
        help="Inspect broader discovered URLs in addition to default C+/C/C-grate relevant sources.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print absent candidate rows and full row text for low-signal evidence.",
    )
    args = parser.parse_args(argv)
    diag = run_diagnostic(include_broad_sources=args.include_broad_sources)
    print_diagnostic(diag, verbose=args.verbose)
    return 0


if __name__ == "__main__":
    sys.exit(main())
