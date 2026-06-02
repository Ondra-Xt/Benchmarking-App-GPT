from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import _extract_final_assemblies, _extract_final_set_details
from tools import diagnose_mplus_base_row_sources as mplus_sources
from tools import report_mplus_compound_assembly_mapping as mplus_mapping

TARGET_MPLUS_ARTICLES = ("9010.81.20", "9010.81.21", "9010.81.22", "9010.81.23")
FLOW_TERMS = (
    "Ablaufleistung",
    "Abflussleistung",
    "Entwässerungsleistung",
    "l/s",
    "Liter pro Sekunde",
    "flow rate",
    "discharge capacity",
    "DIN EN 1253",
)
FLOW_VALUE_RE = re.compile(
    r"(?P<value>\d+(?:[,.]\d+)?)\s*(?:l\s*/\s*s|liter\s+pro\s+sekunde|lit(?:er)?/s)",
    re.IGNORECASE,
)
TERM_RE = re.compile(
    r"ablaufleistung|abflussleistung|entwässerungsleistung|entwaesserungsleistung|l\s*/\s*s|liter pro sekunde|flow rate|discharge capacity|DIN EN 1253",
    re.IGNORECASE,
)
HYDRAULIC_LINK_RE = re.compile(
    r"m\+|mplus|showerdrain|hydraul|ablaufleistung|abflussleistung|entwässer|entwaesser|din[-\s]*en[-\s]*1253|techn|planung|catalog|katalog",
    re.IGNORECASE,
)
ARTICLE_DIGITS_RE = re.compile(r"\b\d{8}\b")


@dataclass(frozen=True)
class FlowSnippet:
    source_url: str
    evidence_type: str
    snippet: str
    flow_values: tuple[str, ...] = field(default_factory=tuple)
    article_numbers: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class DownloadEvidence:
    source_url: str
    href: str
    link_text: str
    evidence_snippet: str


@dataclass(frozen=True)
class ArticleFlowEvidence:
    article_number: str
    proposed_product_id: str
    water_seal_mm: str
    outlet_dn: str
    existing_flow_rate_lps: str
    discovered_flow_rate_lps_candidates: tuple[str, ...]
    source_url: str
    evidence_snippet: str
    evidence_type: str
    confidence: str
    blocking_reason: str
    article_found_in_source_text: bool


@dataclass(frozen=True)
class FlowRiskChecks:
    conflicting_flow_values_by_article: dict[str, tuple[str, ...]] = field(default_factory=dict)
    family_level_value_treated_as_article_level: tuple[str, ...] = field(default_factory=tuple)
    flow_value_without_source_url: tuple[str, ...] = field(default_factory=tuple)
    flow_value_without_evidence_snippet: tuple[str, ...] = field(default_factory=tuple)
    target_articles_missing_from_current_mapping: tuple[str, ...] = field(default_factory=tuple)
    target_articles_not_found_in_source_text: tuple[str, ...] = field(default_factory=tuple)
    production_output_modified: bool = False


@dataclass(frozen=True)
class MPlusFlowRateDiagnostic:
    sheet_counts: dict[str, int]
    target_articles: tuple[str, ...]
    source_urls_inspected: tuple[str, ...]
    flow_snippets: tuple[FlowSnippet, ...]
    per_article_evidence: tuple[ArticleFlowEvidence, ...]
    download_links: tuple[DownloadEvidence, ...]
    safe_to_fill_mplus_flow_rate_lps: bool
    blocking_reason: str
    recommended_next_action: str
    risk_checks: FlowRiskChecks
    production_behavior_changed: bool = False


def _clean(value: Any) -> str:
    return mplus_sources._clean_text(value)


def _article_digits(article_number: str) -> str:
    return mplus_sources._digits_only(article_number)


def _article_variants(article_number: str) -> tuple[str, ...]:
    digits = _article_digits(article_number)
    dotted = f"{digits[:4]}.{digits[4:6]}.{digits[6:]}" if len(digits) == 8 else article_number
    return tuple(dict.fromkeys((article_number, dotted, digits)))


def _normalise_flow_value(value: str) -> str:
    cleaned = _clean(value).replace(",", ".")
    if "." in cleaned:
        cleaned = cleaned.rstrip("0").rstrip(".")
    return cleaned


def _extract_flow_values(text: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(_normalise_flow_value(match.group("value")) for match in FLOW_VALUE_RE.finditer(text or "")))


def _extract_article_numbers(text: str) -> tuple[str, ...]:
    found: list[str] = []
    for match in mplus_sources.ARTICLE_RE.finditer(text or ""):
        digits = _article_digits(match.group(0))
        if len(digits) == 8:
            found.append(f"{digits[:4]}.{digits[4:6]}.{digits[6:]}")
    for match in ARTICLE_DIGITS_RE.finditer(text or ""):
        digits = match.group(0)
        found.append(f"{digits[:4]}.{digits[4:6]}.{digits[6:]}")
    return tuple(dict.fromkeys(found))


def _snippet_around(text: str, start: int, end: int, *, before: int = 180, after: int = 260) -> str:
    return _clean((text or "")[max(0, start - before) : min(len(text or ""), end + after)])


def _term_snippets_from_text(source_url: str, text: str, evidence_type: str) -> tuple[FlowSnippet, ...]:
    snippets: list[FlowSnippet] = []
    seen: set[str] = set()
    for match in TERM_RE.finditer(text or ""):
        snippet = _snippet_around(text, match.start(), match.end())
        if snippet in seen:
            continue
        seen.add(snippet)
        snippets.append(
            FlowSnippet(
                source_url=source_url,
                evidence_type=evidence_type,
                snippet=snippet,
                flow_values=_extract_flow_values(snippet),
                article_numbers=_extract_article_numbers(snippet),
            )
        )
    return tuple(snippets)


def _html_table_rows(soup: BeautifulSoup) -> Iterable[str]:
    for table in soup.select("table"):
        headers = [_clean(cell.get_text(" ", strip=True)) for cell in table.select("thead th, tr th")]
        header_text = " | ".join(headers)
        for tr in table.select("tr"):
            cells = tr.select("td")
            if not cells:
                continue
            row_text = " | ".join(_clean(cell.get_text(" ", strip=True)) for cell in cells)
            yield _clean(f"{header_text} | {row_text}" if header_text else row_text)


def _extract_download_links(source_url: str, soup: BeautifulSoup) -> tuple[DownloadEvidence, ...]:
    links: list[DownloadEvidence] = []
    seen: set[str] = set()
    for link in soup.select("a[href]"):
        href = urljoin(source_url, link.get("href") or "")
        text = _clean(link.get_text(" ", strip=True))
        raw = _clean(f"{text} {href}")
        if not href or not re.search(r"\.(?:pdf|xlsx?|docx?)(?:$|[?#])", href, re.IGNORECASE):
            continue
        if not HYDRAULIC_LINK_RE.search(raw):
            continue
        key = href.split("#", 1)[0]
        if key in seen:
            continue
        seen.add(key)
        links.append(DownloadEvidence(source_url=source_url, href=href, link_text=text, evidence_snippet=raw[:500]))
    return tuple(links)


def inspect_source_url(source_url: str) -> tuple[str, tuple[FlowSnippet, ...], tuple[DownloadEvidence, ...], str]:
    status, final_url, html, error = aco._safe_get_text(source_url, timeout=35)
    final_url = mplus_sources._canonical_url(final_url or source_url)
    if status != 200 or not html:
        snippet = FlowSnippet(
            source_url=final_url,
            evidence_type="fetch_error",
            snippet=f"fetch failed: status={status}; error={error}",
            flow_values=(),
            article_numbers=(),
        )
        return final_url, (snippet,), (), ""

    soup = BeautifulSoup(html, "lxml")
    main = soup.select_one("main") or soup
    flat_text = _clean(main.get_text(" ", strip=True))
    snippets: list[FlowSnippet] = []
    for row_text in _html_table_rows(soup):
        if TERM_RE.search(row_text) or any(variant in row_text for article in TARGET_MPLUS_ARTICLES for variant in _article_variants(article)):
            snippets.extend(_term_snippets_from_text(final_url, row_text, "explicit_article_table"))
            if not TERM_RE.search(row_text) and any(variant in row_text for article in TARGET_MPLUS_ARTICLES for variant in _article_variants(article)):
                snippets.append(
                    FlowSnippet(
                        source_url=final_url,
                        evidence_type="explicit_article_table",
                        snippet=row_text[:500],
                        flow_values=_extract_flow_values(row_text),
                        article_numbers=_extract_article_numbers(row_text),
                    )
                )
    snippets.extend(_term_snippets_from_text(final_url, flat_text, "source_text"))

    deduped: list[FlowSnippet] = []
    seen_snippets: set[tuple[str, str]] = set()
    for snippet in snippets:
        key = (snippet.evidence_type, snippet.snippet)
        if key in seen_snippets:
            continue
        seen_snippets.add(key)
        deduped.append(snippet)
    return final_url, tuple(deduped), _extract_download_links(final_url, soup), flat_text


def _source_urls_from_mapping(report: mplus_mapping.MPlusCompoundMappingReport) -> tuple[str, ...]:
    urls: list[str] = []
    urls.extend(mplus_sources.KNOWN_MPLUS_SOURCE_URLS)
    for row in report.proposed_mappings:
        urls.extend((row.source_url_channel_body, row.source_url_drain_body, row.source_url_grate))
    for row in report.generic_drain_body_pages:
        urls.append(row.source_url)
    canonical_urls = []
    for url in urls:
        canonical = mplus_sources._canonical_url(url.replace("/produktee/", "/produkte/"))
        if canonical:
            canonical_urls.append(canonical)
    return tuple(dict.fromkeys(canonical_urls))


def _article_source_text_found(article: str, source_text_by_url: dict[str, str]) -> bool:
    variants = _article_variants(article)
    return any(any(variant in text for variant in variants) for text in source_text_by_url.values())


def _candidate_rows_for_article(report: mplus_mapping.MPlusCompoundMappingReport, article: str) -> tuple[mplus_mapping.ProposedCompoundMappingRow, ...]:
    variants = set(_article_variants(article))
    return tuple(row for row in report.proposed_mappings if row.drain_body_article_number in variants)


def _evidence_type_for_article(snippet: FlowSnippet, article: str) -> str:
    articles = set(snippet.article_numbers)
    has_article = article in articles or bool(articles & set(_article_variants(article)))
    if snippet.evidence_type == "explicit_article_table" and has_article:
        return "explicit_article_table"
    if has_article and snippet.evidence_type == "source_text":
        return "explicit_article_detail"
    if snippet.flow_values and not has_article:
        return "ambiguous"
    return "absent"


def _classify_article_evidence(
    article: str,
    rows: tuple[mplus_mapping.ProposedCompoundMappingRow, ...],
    snippets: tuple[FlowSnippet, ...],
    source_text_by_url: dict[str, str],
) -> ArticleFlowEvidence:
    proposed_product_id = rows[0].drain_body_id if rows else ""
    water_seal_mm = rows[0].water_seal_mm if rows else ""
    outlet_dn = rows[0].outlet_dn if rows else ""
    existing_flow = rows[0].flow_rate_lps if rows else ""
    article_found = _article_source_text_found(article, source_text_by_url)

    explicit: list[FlowSnippet] = []
    ambiguous: list[FlowSnippet] = []
    for snippet in snippets:
        if not snippet.flow_values:
            continue
        evidence_type = _evidence_type_for_article(snippet, article)
        if evidence_type in {"explicit_article_table", "explicit_article_detail"}:
            explicit.append(snippet)
        elif evidence_type == "ambiguous":
            ambiguous.append(snippet)

    chosen: FlowSnippet | None = explicit[0] if explicit else (ambiguous[0] if ambiguous else None)
    values = tuple(dict.fromkeys(value for snippet in (explicit or ambiguous) for value in snippet.flow_values))
    if explicit:
        evidence_type = _evidence_type_for_article(chosen, article) if chosen else "explicit_article_detail"
        confidence = "high" if evidence_type == "explicit_article_table" else "medium"
        blocking = "explicit source-backed flow_rate_lps candidate found; manual review required before any non-diagnostic fill."
    elif ambiguous:
        evidence_type = "ambiguous"
        confidence = "low"
        blocking = "flow/discharge value is generic family-level or not explicitly tied to the target article/drain body."
    else:
        evidence_type = "absent"
        confidence = "absent"
        blocking = "no explicit source-backed flow_rate_lps evidence found for the target article."

    return ArticleFlowEvidence(
        article_number=article,
        proposed_product_id=proposed_product_id,
        water_seal_mm=water_seal_mm,
        outlet_dn=outlet_dn,
        existing_flow_rate_lps=existing_flow,
        discovered_flow_rate_lps_candidates=values,
        source_url=chosen.source_url if chosen else "",
        evidence_snippet=chosen.snippet if chosen else "",
        evidence_type=evidence_type,
        confidence=confidence,
        blocking_reason=blocking,
        article_found_in_source_text=article_found,
    )


def _risk_checks(
    evidence_rows: tuple[ArticleFlowEvidence, ...],
    report: mplus_mapping.MPlusCompoundMappingReport,
) -> FlowRiskChecks:
    conflicting = {
        row.article_number: row.discovered_flow_rate_lps_candidates
        for row in evidence_rows
        if len(row.discovered_flow_rate_lps_candidates) > 1
    }
    target_mapped = {row.drain_body_article_number for row in report.proposed_mappings}
    return FlowRiskChecks(
        conflicting_flow_values_by_article=conflicting,
        family_level_value_treated_as_article_level=tuple(
            row.article_number
            for row in evidence_rows
            if row.evidence_type == "ambiguous" and row.confidence not in {"low", "absent"}
        ),
        flow_value_without_source_url=tuple(
            row.article_number for row in evidence_rows if row.discovered_flow_rate_lps_candidates and not row.source_url
        ),
        flow_value_without_evidence_snippet=tuple(
            row.article_number for row in evidence_rows if row.discovered_flow_rate_lps_candidates and not row.evidence_snippet
        ),
        target_articles_missing_from_current_mapping=tuple(
            article for article in TARGET_MPLUS_ARTICLES if article not in target_mapped
        ),
        target_articles_not_found_in_source_text=tuple(
            row.article_number for row in evidence_rows if not row.article_found_in_source_text
        ),
        production_output_modified=False,
    )


def build_diagnostic(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    components: pd.DataFrame,
    bom_options: pd.DataFrame,
    final_assemblies: pd.DataFrame | None = None,
    final_set_details: pd.DataFrame | None = None,
) -> MPlusFlowRateDiagnostic:
    final_assemblies = pd.DataFrame() if final_assemblies is None else final_assemblies
    final_set_details = pd.DataFrame() if final_set_details is None else final_set_details
    mapping_report = mplus_mapping.build_report(
        candidates_all,
        products,
        comparison,
        components,
        bom_options,
        final_assemblies,
        final_set_details,
    )
    source_urls = _source_urls_from_mapping(mapping_report)

    inspected: list[str] = []
    all_snippets: list[FlowSnippet] = []
    all_downloads: list[DownloadEvidence] = []
    source_text_by_url: dict[str, str] = {}
    for url in source_urls:
        final_url, snippets, downloads, flat_text = inspect_source_url(url)
        inspected.append(final_url)
        all_snippets.extend(snippets)
        all_downloads.extend(downloads)
        source_text_by_url[final_url] = flat_text

    evidence_rows = tuple(
        _classify_article_evidence(
            article,
            _candidate_rows_for_article(mapping_report, article),
            tuple(all_snippets),
            source_text_by_url,
        )
        for article in TARGET_MPLUS_ARTICLES
    )
    safe_to_fill = all(row.evidence_type in {"explicit_article_table", "explicit_article_detail"} for row in evidence_rows)
    safe_to_fill = safe_to_fill and all(len(row.discovered_flow_rate_lps_candidates) == 1 for row in evidence_rows)
    blocking = (
        "explicit article-level or clearly applicable drain-body-level source evidence found for every target article"
        if safe_to_fill
        else "one or more M+ drain body articles lack explicit source-backed flow_rate_lps evidence; keep mappings blocked by flow_rate_lps."
    )
    risk_checks = _risk_checks(evidence_rows, mapping_report)
    if any(
        (
            risk_checks.conflicting_flow_values_by_article,
            risk_checks.family_level_value_treated_as_article_level,
            risk_checks.flow_value_without_source_url,
            risk_checks.flow_value_without_evidence_snippet,
            risk_checks.target_articles_missing_from_current_mapping,
        )
    ):
        safe_to_fill = False

    return MPlusFlowRateDiagnostic(
        sheet_counts=mapping_report.sheet_counts,
        target_articles=TARGET_MPLUS_ARTICLES,
        source_urls_inspected=tuple(dict.fromkeys(inspected)),
        flow_snippets=tuple(all_snippets),
        per_article_evidence=evidence_rows,
        download_links=tuple(dict.fromkeys(all_downloads)),
        safe_to_fill_mplus_flow_rate_lps=safe_to_fill,
        blocking_reason=blocking,
        recommended_next_action=(
            "Parse identified PDF/download evidence or request an explicit ACO technical article table before writing flow_rate_lps."
            if not safe_to_fill
            else "Review explicit diagnostic evidence, then implement a separate non-diagnostic patch if production fill is desired."
        ),
        risk_checks=risk_checks,
        production_behavior_changed=False,
    )


def _print_count_dict(title: str, values: dict[str, int]) -> None:
    print(f"\n{title}:")
    for key, value in values.items():
        print(f"- {key}: {value}")


def print_report(diag: MPlusFlowRateDiagnostic) -> None:
    print("ACO ShowerDrain M+ flow_rate_lps source investigation diagnostic (read-only; no generation)")
    _print_count_dict("Input sheet/frame counts", diag.sheet_counts)

    print("\nTarget M+ articles:")
    for article in diag.target_articles:
        print(f"- {article}")

    print("\nSource URLs inspected:")
    for url in diag.source_urls_inspected:
        print(f"- {url}")

    print("\nFlow/discharge snippets found:")
    if not diag.flow_snippets:
        print("- none")
    for snippet in diag.flow_snippets:
        print(
            "- "
            f"evidence_type={snippet.evidence_type}; "
            f"flow_values={','.join(snippet.flow_values) or '(none)'}; "
            f"articles={','.join(snippet.article_numbers) or '(none)'}; "
            f"source_url={snippet.source_url}; snippet={snippet.snippet[:320]}"
        )

    print("\nPer-article flow evidence table:")
    for row in diag.per_article_evidence:
        print(
            "- "
            f"article_number={row.article_number}; "
            f"proposed_product_id={row.proposed_product_id or '(missing)'}; "
            f"water_seal_mm={row.water_seal_mm or '(missing)'}; "
            f"outlet_dn={row.outlet_dn or '(missing)'}; "
            f"existing_flow_rate_lps={row.existing_flow_rate_lps or '(missing)'}; "
            f"discovered_flow_rate_lps_candidates={','.join(row.discovered_flow_rate_lps_candidates) or '(none)'}; "
            f"source_url={row.source_url or '(missing)'}; "
            f"evidence_type={row.evidence_type}; confidence={row.confidence}; "
            f"article_found_in_source_text={row.article_found_in_source_text}; "
            f"evidence_snippet={row.evidence_snippet[:320] or '(missing)'}"
        )

    print("\nPDF/download links related to M+ hydraulics:")
    if not diag.download_links:
        print("- none")
    for link in diag.download_links:
        print(f"- source_url={link.source_url}; href={link.href}; link_text={link.link_text or '(missing)'}")

    r = diag.risk_checks
    print("\nRisk checks:")
    conflicts = "; ".join(f"{article}={','.join(values)}" for article, values in r.conflicting_flow_values_by_article.items())
    print(f"- multiple conflicting flow values for the same article: {conflicts or 'none'}")
    print(f"- family-level value incorrectly treated as article-level value: {', '.join(r.family_level_value_treated_as_article_level) or 'none'}")
    print(f"- flow value without source URL: {', '.join(r.flow_value_without_source_url) or 'none'}")
    print(f"- flow value without evidence snippet: {', '.join(r.flow_value_without_evidence_snippet) or 'none'}")
    print(f"- target article missing from current mapping: {', '.join(r.target_articles_missing_from_current_mapping) or 'none'}")
    print(f"- article not found in source text: {', '.join(r.target_articles_not_found_in_source_text) or 'none'}")
    print(f"- any production output modified: {'yes' if r.production_output_modified else 'no'}")

    print("\nFinal flow-rate source assessment:")
    print(f"- safe_to_fill_mplus_flow_rate_lps: {diag.safe_to_fill_mplus_flow_rate_lps}")
    print(f"- blocking_reason: {diag.blocking_reason}")
    print(f"- recommended_next_action: {diag.recommended_next_action}")
    print("production behavior changed: no")


def main() -> int:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(registry_rows)
    products, comparison, components, _evidence, bom_options = pipeline.run_update(candidates_all, default_config())
    final_assemblies = _extract_final_assemblies(products)
    final_set_details = _extract_final_set_details(final_assemblies, bom_options, components)
    diag = build_diagnostic(candidates_all, products, comparison, components, bom_options, final_assemblies, final_set_details)
    print_report(diag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
