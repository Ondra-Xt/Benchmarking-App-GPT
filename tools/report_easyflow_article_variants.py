from __future__ import annotations

import csv
import re
import sys
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import urlparse

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from tools import diagnose_easyflow_article_variants as diag

BASE_PRODUCT_ID = diag.BASE_ID
EASYFLOW_BASE_URLS = (
    f"{aco.BASE}{aco.BADABLAEUFE_SCOPE}aco-easyflow/komplettablaeufe-aco-easyflow-dn-50/",
    f"{aco.BASE}{aco.BADABLAEUFE_SCOPE}aco-easyflow/einzelablaeufe-aco-easyflow-dn-50/",
)

VARIANT_COLUMNS = (
    "base_product_id",
    "article_number",
    "source_url",
    "variant_type",
    "water_seal_mm",
    "outlet_dn",
    "flow_rate_lps",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "cutout_mm",
    "side_inlet",
    "row_text",
    "attribution_status",
)

CANDIDATE_BODY_VARIANT = "candidate_body_variant"
EXCLUDED_ACCESSORY_VARIANT = "excluded_accessory_variant"
EXCLUDED_GRATE_VARIANT = "excluded_grate_variant"
EXCLUDED_EASYFLOWPLUS = "excluded_easyflowplus"

CANDIDATE_BODY_PATH_FRAGMENTS = (
    "/easyflow/komplettablaeufe-aco-easyflow-dn-50/",
    "/easyflow/einzelablaeufe-aco-easyflow-dn-50/",
    "/aco-easyflow/komplettablaeufe-aco-easyflow-dn-50/",
    "/aco-easyflow/einzelablaeufe-aco-easyflow-dn-50/",
)
CUTOUT_RE = re.compile(r"(?:(?:ausspar|cutout|recess)[^0-9]{0,30})?(\d{2,4})\s*[x×]\s*(\d{2,4})(?:\s*[x×]\s*(\d{2,4}))?\s*mm", re.IGNORECASE)
SIDE_INLET_RE = re.compile(r"seit(?:en)?(?:zulauf|einlauf)|side\s*inlet", re.IGNORECASE)
NO_SIDE_INLET_RE = re.compile(r"ohne\s+seit(?:en)?(?:zulauf|einlauf)|without\s+side\s*inlet", re.IGNORECASE)


@dataclass(frozen=True)
class NormalizedVariantRow:
    base_product_id: str
    article_number: str
    source_url: str
    variant_type: str
    water_seal_mm: int | None
    outlet_dn: str
    flow_rate_lps: float | None
    height_adj_min_mm: int | None
    height_adj_max_mm: int | None
    cutout_mm: str
    side_inlet: str
    row_text: str
    attribution_status: str

    def as_dict(self) -> dict[str, Any]:
        return {column: getattr(self, column) for column in VARIANT_COLUMNS}


@dataclass(frozen=True)
class VariantReport:
    variant_rows: list[NormalizedVariantRow]
    inspected_urls: list[str]
    base_row: dict[str, str]
    counts: dict[str, int]
    matching_ws50_dn50_count: int
    matching_article_numbers: list[str]
    distinct_flow_values: list[float]
    distinct_height_ranges: list[str]
    unique_attribution_possible: bool
    attribution_status: str
    attribution_reason: str


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _lower_url_text(*values: Any) -> str:
    return " ".join(str(value or "") for value in values).lower()


def _is_easyflowplus(*values: Any) -> bool:
    text = _lower_url_text(*values)
    return "easyflowplus" in text or "easyflow+" in text or "easyflow-plus" in text


def _is_easyflow_source_url(url: str) -> bool:
    parsed = urlparse(diag.canonical_url(url))
    path = (parsed.path or "").lower()
    return "/badablaeufe/" in path and "easyflow" in path


def _source_urls_from_pipeline(registry: pd.DataFrame, debug: Any, rows: Iterable[pd.Series | None]) -> list[str]:
    urls: list[str] = []

    def add(raw: str) -> None:
        url = diag.canonical_url(raw)
        if not url or url in urls:
            return
        if not _is_easyflow_source_url(url):
            return
        # Easyflow+ rows are intentionally not inspected; the excluded class remains
        # available for normalized rows supplied directly to tests or future diagnostics.
        if _is_easyflowplus(url):
            return
        urls.append(url)

    for url in EASYFLOW_BASE_URLS:
        add(url)

    for row in rows:
        for url in diag.urls_from_row(row):
            add(url)

    if registry is not None and not registry.empty:
        for _, row in registry.iterrows():
            row_text = _lower_url_text(row.get("product_id", ""), row.get("product_name", ""), row.get("product_url", ""), row.get("sources", ""))
            if "easyflow" not in row_text:
                continue
            for url in diag.urls_from_row(row):
                add(url)

    if isinstance(debug, list):
        for item in debug:
            if not isinstance(item, dict):
                continue
            item_text = _lower_url_text(item.get("final_url", ""), item.get("seed_url", ""), item.get("error", ""))
            if "easyflow" not in item_text or _is_easyflowplus(item_text):
                continue
            for key in ("final_url", "seed_url"):
                add(str(item.get(key, "")))

    return urls


def classify_variant_row(source_url: str, row_text: str) -> str:
    text = _lower_url_text(source_url, row_text)
    if _is_easyflowplus(text):
        return EXCLUDED_EASYFLOWPLUS
    if "design-roste" in text or "design-rost" in text or re.search(r"\brost(?:e)?\b|\bgrate\b", text):
        return EXCLUDED_GRATE_VARIANT
    if "aufsatzstuecke" in text or "aufsatzstück" in text or "aufsatzstueck" in text or "aufsatzstuck" in text:
        return EXCLUDED_ACCESSORY_VARIANT
    path = (urlparse(diag.canonical_url(source_url)).path or "").lower()
    if any(fragment in path for fragment in CANDIDATE_BODY_PATH_FRAGMENTS):
        return CANDIDATE_BODY_VARIANT
    return EXCLUDED_ACCESSORY_VARIANT


def _extract_cutout(text: str) -> str:
    match = CUTOUT_RE.search(text or "")
    if not match:
        return ""
    parts = [group for group in match.groups() if group]
    return " x ".join(parts) + " mm"


def _extract_side_inlet(text: str) -> str:
    if NO_SIDE_INLET_RE.search(text or ""):
        return "false"
    if SIDE_INLET_RE.search(text or ""):
        return "true"
    return ""


def _attribution_status_for_variant(candidate: diag.ArticleVariantCandidate, variant_type: str) -> str:
    if variant_type != CANDIDATE_BODY_VARIANT:
        return "excluded_not_body_variant"
    if diag.candidate_matches_base(candidate):
        return "matches_current_ws50_dn50_base_facts"
    return "candidate_body_variant_not_current_base_match"


def normalize_candidate(candidate: diag.ArticleVariantCandidate) -> NormalizedVariantRow:
    row_text = _clean_text(candidate.row_description)
    descriptor_text = _clean_text(candidate.dimensions_recess_cutout_descriptors)
    combined_text = _clean_text(f"{row_text} {descriptor_text}")
    variant_type = classify_variant_row(candidate.source_url, combined_text)
    return NormalizedVariantRow(
        base_product_id=BASE_PRODUCT_ID,
        article_number=candidate.article_number,
        source_url=candidate.source_url,
        variant_type=variant_type,
        water_seal_mm=candidate.water_seal_mm,
        outlet_dn=candidate.outlet_dn,
        flow_rate_lps=candidate.flow_rate_lps,
        height_adj_min_mm=candidate.height_adj_min_mm,
        height_adj_max_mm=candidate.height_adj_max_mm,
        cutout_mm=_extract_cutout(combined_text),
        side_inlet=_extract_side_inlet(combined_text),
        row_text=row_text,
        attribution_status=_attribution_status_for_variant(candidate, variant_type),
    )


def fetch_normalized_variant_rows(urls: Iterable[str]) -> tuple[list[str], list[NormalizedVariantRow]]:
    inspected: list[str] = []
    rows: list[NormalizedVariantRow] = []
    seen: set[tuple[str, str, str]] = set()
    for raw_url in urls:
        url = diag.canonical_url(raw_url)
        if _is_easyflowplus(url):
            continue
        status, final_url, html, _err = aco._safe_get_text(url, timeout=35)
        final = diag.canonical_url(final_url or url)
        if final not in inspected:
            inspected.append(final)
        if status != 200 or not html:
            continue
        if _is_easyflowplus(final, html[:4000]):
            continue
        for candidate in diag.parse_article_variant_candidates(html, final):
            row = normalize_candidate(candidate)
            key = (row.article_number, row.source_url, row.row_text)
            if key not in seen:
                seen.add(key)
                rows.append(row)
    return inspected, rows


def _row_summary(row: pd.Series | None) -> dict[str, str]:
    return diag._row_summary(row)


def build_variant_report() -> VariantReport:
    registry_rows, debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, excluded, evidence, bom = pipeline.run_update(registry, default_config())

    frames = {"Products": products, "Comparison": comparison, "Candidates_All": registry, "Components": excluded}
    _base_location, base_row = diag.locate_product_id(BASE_PRODUCT_ID, frames)
    source_urls = _source_urls_from_pipeline(registry, debug, [base_row])
    inspected_urls, variant_rows = fetch_normalized_variant_rows(source_urls)

    body_candidates = [row for row in variant_rows if row.variant_type == CANDIDATE_BODY_VARIANT]
    matching = [row for row in body_candidates if row.water_seal_mm == 50 and "DN50" in (row.outlet_dn or "").upper().split("/")]
    distinct_flow_values = sorted({float(row.flow_rate_lps) for row in matching if row.flow_rate_lps is not None})
    distinct_height_ranges = sorted({f"{row.height_adj_min_mm}-{row.height_adj_max_mm}" for row in matching if row.height_adj_min_mm is not None and row.height_adj_max_mm is not None})
    unique = len(matching) == 1
    if not variant_rows:
        attribution_status = "parser_insufficient"
        reason = "No normalized Easyflow article/variant rows could be parsed from inspected source pages."
    elif unique:
        attribution_status = "exact_article_match"
        reason = "A single normalized drain-body variant matches the current Easyflow WS50/DN50 base facts."
    elif len(matching) > 1:
        attribution_status = "multiple_candidate_articles"
        reason = "Multiple normalized Easyflow drain-body variants match the current WS50/DN50 base facts; do not infer one article at base-row granularity."
    else:
        attribution_status = "no_article_match"
        reason = "Normalized article rows exist, but no drain-body variant matches both WS50 and DN50."

    counts = {
        "Products": len(products),
        "Comparison": len(comparison),
        "Candidates_All": len(registry),
        "Components": len(excluded),
        "BOM_Options": len(bom),
        "Final_Assemblies": 0,
        "normalized_variant_rows": len(variant_rows),
        CANDIDATE_BODY_VARIANT: sum(1 for row in variant_rows if row.variant_type == CANDIDATE_BODY_VARIANT),
        EXCLUDED_ACCESSORY_VARIANT: sum(1 for row in variant_rows if row.variant_type == EXCLUDED_ACCESSORY_VARIANT),
        EXCLUDED_GRATE_VARIANT: sum(1 for row in variant_rows if row.variant_type == EXCLUDED_GRATE_VARIANT),
        EXCLUDED_EASYFLOWPLUS: sum(1 for row in variant_rows if row.variant_type == EXCLUDED_EASYFLOWPLUS),
        "matching_ws50_dn50_body_variants": len(matching),
    }

    return VariantReport(
        variant_rows=variant_rows,
        inspected_urls=inspected_urls,
        base_row=_row_summary(base_row),
        counts=counts,
        matching_ws50_dn50_count=len(matching),
        matching_article_numbers=[row.article_number for row in matching],
        distinct_flow_values=distinct_flow_values,
        distinct_height_ranges=distinct_height_ranges,
        unique_attribution_possible=unique,
        attribution_status=attribution_status,
        attribution_reason=reason,
    )


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def _print_markdown_table(rows: list[NormalizedVariantRow]) -> None:
    print("| " + " | ".join(VARIANT_COLUMNS) + " |")
    print("|" + "|".join("---" for _ in VARIANT_COLUMNS) + "|")
    for row in rows:
        values = [str(_fmt(row.as_dict()[column])).replace("|", "\\|") for column in VARIANT_COLUMNS]
        print("| " + " | ".join(values) + " |")


def write_variant_csv(rows: list[NormalizedVariantRow], stream: Any = sys.stdout) -> None:
    writer = csv.DictWriter(stream, fieldnames=list(VARIANT_COLUMNS), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _fmt(value) for key, value in row.as_dict().items()})


def print_report(report: VariantReport) -> None:
    print("Normalized Easyflow article variant diagnostic")
    print("\nInspected source URLs (Easyflow only; Easyflow+ skipped):")
    if report.inspected_urls:
        for url in report.inspected_urls:
            print(f"- {url}")
    else:
        print("- none")

    print("\nNormalized Easyflow variant table:")
    if report.variant_rows:
        _print_markdown_table(report.variant_rows)
    else:
        print("- none")

    print("\nMatching WS50/DN50 body variants:")
    print(f"- matching WS50/DN50 candidate count: {report.matching_ws50_dn50_count}")
    print(f"- article numbers: {', '.join(report.matching_article_numbers) if report.matching_article_numbers else 'none'}")
    print(f"- distinct flow values: {', '.join(_fmt(v) for v in report.distinct_flow_values) if report.distinct_flow_values else 'unknown'}")
    print(f"- distinct height ranges: {', '.join(report.distinct_height_ranges) if report.distinct_height_ranges else 'unknown'}")
    print(f"- unique attribution possible: {report.unique_attribution_possible}")
    print(f"- attribution status: {report.attribution_status}")
    print(f"- attribution reason: {report.attribution_reason}")

    print("\nExcluded row counts:")
    print(f"- accessories: {report.counts[EXCLUDED_ACCESSORY_VARIANT]}")
    print(f"- grates: {report.counts[EXCLUDED_GRATE_VARIANT]}")
    print(f"- Easyflow+: {report.counts[EXCLUDED_EASYFLOWPLUS]}")

    print("\nProduction behavior:")
    print("- diagnostic-only; no Products, Comparison, Final_Assemblies, BOM_Options, scoring, validator baseline, connector, pipeline, app, or XLSX export behavior is modified")
    print("- flow_rate_lps and height adjustment values are reported only in this diagnostic table and are not written to Products")


def main() -> int:
    report = build_variant_report()
    print_report(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
