from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src import excel_export
from src.canonical_aco_export import build_canonical_aco_frames
from src.config import default_config
from tools import report_easyflow_article_variants

EASYFLOW_FAMILY = "easyflow"
CANDIDATE_VARIANT_TYPE = "candidate_body_variant"
EVIDENCE_ONLY_STATUS = "candidate_variant"
AMBIGUOUS_FIELDS = ("flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm")
TECHNICAL_FIELDS = (
    "water_seal_mm",
    "outlet_dn",
    "flow_rate_lps",
    "installation_height_mm",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "side_inlet",
    "variant_condition",
)
ARTICLE_FIELDS = ("base_article_number", "article_number")
ASSEMBLY_ID_FIELDS = ("assembled_product_id", "product_id")
COMPONENT_ID_FIELDS = ("selected_component_id", "component_id")

ATTRIBUTION_STATES = {
    "explicit_unique_article_match",
    "unique_source_backed_technical_match",
    "ambiguous_multiple_article_candidates",
    "insufficient_article_level_evidence",
    "conflicting_article_level_evidence",
    "invalid_source_record",
}


@dataclass(frozen=True)
class SourceEvidenceRecord:
    article_number: str
    original_article_text: str
    source_url: str
    source_record: str
    flow_rate_lps: float | None
    water_seal_mm: int | None
    outlet_dn: str
    installation_height_mm: int | None
    height_adj_min_mm: int | None
    height_adj_max_mm: int | None
    side_inlet: str
    variant_condition: str
    evidence_type: str
    confidence: str


@dataclass(frozen=True)
class AssemblyAttribution:
    assembled_product_id: str
    product_family: str
    base_product_id: str
    selected_component_id: str
    candidate_article_numbers: tuple[str, ...]
    explicit_article_number: str
    attributed_article_number: str
    attribution_method: str
    attribution_confidence: str
    matched_source_url: str
    matched_source_record: str
    matched_technical_fields: tuple[str, ...]
    conflicting_technical_fields: tuple[str, ...]
    flow_rate_lps: float | None
    height_adj_min_mm: int | None
    height_adj_max_mm: int | None
    article_level_attribution_found: bool
    ready_for_article_promotion_review: bool
    blocking_reason: str
    recommended_next_action: str

    @property
    def component_id(self) -> str:
        return self.selected_component_id

    @property
    def status(self) -> str:
        return self.attribution_method

    @property
    def unique_article_attribution_found(self) -> bool:
        return self.article_level_attribution_found

    @property
    def matching_article_numbers(self) -> tuple[str, ...]:
        return self.candidate_article_numbers


@dataclass(frozen=True)
class AttributionReport:
    assembly_results: tuple[AssemblyAttribution, ...]
    source_evidence_inventory: tuple[SourceEvidenceRecord, ...]
    sheet_counts: dict[str, int]
    workbook_byte_unchanged: bool | None = None

    @property
    def unique_attribution_found_for_all(self) -> bool:
        return bool(self.assembly_results) and all(
            row.article_level_attribution_found for row in self.assembly_results
        )

    @property
    def overall_status(self) -> str:
        if any(row.ready_for_article_promotion_review for row in self.assembly_results):
            return "article_level_attribution_ready_for_manual_review"
        return "article_level_attribution_not_resolved"

    def diagnostic_frame(self) -> pd.DataFrame:
        return pd.DataFrame([asdict(row) for row in self.assembly_results])

    def inventory_frame(self) -> pd.DataFrame:
        return pd.DataFrame([asdict(row) for row in self.source_evidence_inventory])


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _normalized_article(value: Any) -> str:
    digits = re.sub(r"\D+", "", _text(value))
    return digits if len(digits) == 8 else ""


def _article_display(value: Any) -> str:
    digits = _normalized_article(value)
    return f"{digits[:4]}.{digits[4:6]}.{digits[6:]}" if digits else _text(value)


def _float_value(value: Any) -> float | None:
    text = _text(value).replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _int_value(value: Any) -> int | None:
    number = _float_value(value)
    return None if number is None else int(number)


def _outlet_tokens(value: Any) -> set[str]:
    return {f"DN{match}" for match in re.findall(r"(?:DN\s*)?(\d{2,3})", _text(value).upper())}


def _canonical_value(field: str, value: Any) -> Any:
    if field == "outlet_dn":
        return tuple(sorted(_outlet_tokens(value)))
    if field in {"water_seal_mm", "installation_height_mm", "height_adj_min_mm", "height_adj_max_mm"}:
        return _int_value(value)
    if field == "flow_rate_lps":
        return _float_value(value)
    return _text(value).lower()


def _has_value(field: str, value: Any) -> bool:
    canonical = _canonical_value(field, value)
    return canonical not in (None, "", (), set())


def _source_record_text(row: pd.Series, fallback_index: Any = "") -> str:
    for field in ("source_record", "row_text", "source_text", "record_text"):
        value = _text(row.get(field))
        if value:
            return value
    article = _article_display(row.get("article_number"))
    return f"Article_Variants row {fallback_index}: {article}".strip()


def _record_article(row: pd.Series) -> tuple[str, str]:
    for field in ARTICLE_FIELDS:
        original = _text(row.get(field))
        normalized = _normalized_article(original)
        if normalized:
            return normalized, original
    return "", ""


def _record_source_url(row: pd.Series) -> str:
    for field in ("source_url", "product_url", "url"):
        value = _text(row.get(field))
        if value:
            return value
    return ""


def _source_backed_easyflow_variants(article_variants: pd.DataFrame | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    variants = pd.DataFrame() if article_variants is None else article_variants.copy(deep=True)
    if variants.empty:
        return variants.copy(), variants.copy()
    required = {"article_number", "source_url", "variant_type", "product_family"}
    if not required.issubset(variants.columns):
        return variants.iloc[0:0].copy(), variants.copy()

    family_mask = variants["product_family"].fillna("").astype(str).str.strip().str.lower().eq(EASYFLOW_FAMILY)
    body_mask = variants["variant_type"].fillna("").astype(str).str.strip().eq(CANDIDATE_VARIANT_TYPE)
    candidate_rows = variants[family_mask & body_mask].copy()
    valid_mask = (
        candidate_rows["article_number"].map(_normalized_article).ne("")
        & candidate_rows["source_url"].fillna("").astype(str).str.strip().ne("")
    )
    if "attribution_status" in candidate_rows.columns:
        valid_mask &= candidate_rows["attribution_status"].fillna("").astype(str).str.strip().eq(EVIDENCE_ONLY_STATUS)
    return candidate_rows[valid_mask].copy(), candidate_rows[~valid_mask].copy()


def _inventory(valid_variants: pd.DataFrame) -> tuple[SourceEvidenceRecord, ...]:
    records: list[SourceEvidenceRecord] = []
    for index, row in valid_variants.iterrows():
        records.append(
            SourceEvidenceRecord(
                article_number=_article_display(row.get("article_number")),
                original_article_text=_text(row.get("article_number")),
                source_url=_record_source_url(row),
                source_record=_source_record_text(row, index),
                flow_rate_lps=_float_value(row.get("flow_rate_lps")),
                water_seal_mm=_int_value(row.get("water_seal_mm")),
                outlet_dn=_text(row.get("outlet_dn")),
                installation_height_mm=_int_value(row.get("installation_height_mm")),
                height_adj_min_mm=_int_value(row.get("height_adj_min_mm")),
                height_adj_max_mm=_int_value(row.get("height_adj_max_mm")),
                side_inlet=_text(row.get("side_inlet")),
                variant_condition=_text(row.get("variant_condition") or row.get("condition")),
                evidence_type=_text(row.get("evidence_type")) or "article_variant_source_record",
                confidence=_text(row.get("confidence")) or "source_backed_candidate",
            )
        )
    return tuple(records)


def _exact_identity_match(row: pd.Series, assembled_id: str, component_id: str) -> bool:
    assembly_values = {_text(row.get(field)) for field in ASSEMBLY_ID_FIELDS if _text(row.get(field))}
    component_values = {_text(row.get(field)) for field in COMPONENT_ID_FIELDS if _text(row.get(field))}
    return (assembled_id in assembly_values) or (bool(component_id) and component_id in component_values)


def _explicit_records(
    assembled_id: str,
    component_id: str,
    assembly: pd.Series,
    detail_rows: pd.DataFrame,
    source_frames: Mapping[str, pd.DataFrame],
) -> list[tuple[str, pd.Series, str]]:
    records: list[tuple[str, pd.Series, str]] = []
    sources: list[tuple[str, pd.DataFrame]] = [
        ("Final_Assemblies", pd.DataFrame([assembly])),
        ("Final_Set_Details", detail_rows),
    ]
    sources.extend((name, frame.copy(deep=True)) for name, frame in source_frames.items() if frame is not None)
    for source_name, frame in sources:
        if frame.empty:
            continue
        for index, row in frame.iterrows():
            article, _original = _record_article(row)
            if not article or not _exact_identity_match(row, assembled_id, component_id):
                continue
            # Explicit attribution must be a primary-source record, not an unsourced
            # production field or an article number embedded in family-level prose.
            if not _record_source_url(row):
                continue
            records.append((source_name, row.copy(), str(index)))
    return records


def _technical_comparison(assembly: pd.Series, candidate: pd.Series) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    matched: list[str] = []
    conflicting: list[str] = []
    missing: list[str] = []
    for field in TECHNICAL_FIELDS:
        assembly_value = assembly.get(field)
        candidate_value = candidate.get(field)
        if not _has_value(field, assembly_value):
            continue
        if not _has_value(field, candidate_value):
            missing.append(field)
            continue
        if _canonical_value(field, assembly_value) == _canonical_value(field, candidate_value):
            matched.append(field)
        else:
            conflicting.append(field)
    return tuple(matched), tuple(conflicting), tuple(missing)


def _technical_candidates(assembly: pd.Series, candidates: pd.DataFrame) -> tuple[pd.DataFrame, tuple[str, ...]]:
    matching_indexes: list[Any] = []
    observed_fields: set[str] = set()
    for index, row in candidates.iterrows():
        matched, conflicting, missing = _technical_comparison(assembly, row)
        observed_fields.update(matched)
        observed_fields.update(conflicting)
        if matched and not conflicting and not missing:
            matching_indexes.append(index)
    return candidates.loc[matching_indexes].copy(), tuple(sorted(observed_fields))


def _distinct_conflicting_fields(candidates: pd.DataFrame) -> tuple[str, ...]:
    conflicts: list[str] = []
    for field in TECHNICAL_FIELDS:
        values = {
            _canonical_value(field, value)
            for value in candidates.get(field, pd.Series(dtype=object))
            if _has_value(field, value)
        }
        if len(values) > 1:
            conflicts.append(field)
    return tuple(conflicts)


def _article_numbers(candidates: pd.DataFrame) -> tuple[str, ...]:
    return tuple(sorted({_article_display(value) for value in candidates.get("article_number", pd.Series(dtype=str)) if _normalized_article(value)}))


def _selected_values(row: pd.Series | None) -> tuple[float | None, int | None, int | None]:
    if row is None:
        return None, None, None
    return (
        _float_value(row.get("flow_rate_lps")),
        _int_value(row.get("height_adj_min_mm")),
        _int_value(row.get("height_adj_max_mm")),
    )


def assess_easyflow_article_attribution(
    final_assemblies: pd.DataFrame,
    final_set_details: pd.DataFrame,
    article_variants: pd.DataFrame,
    *,
    source_frames: Mapping[str, pd.DataFrame] | None = None,
) -> AttributionReport:
    """Build a read-only, fail-closed Easyflow article-attribution diagnostic."""
    assemblies = pd.DataFrame() if final_assemblies is None else final_assemblies.copy(deep=True)
    details = pd.DataFrame() if final_set_details is None else final_set_details.copy(deep=True)
    source_frames = {name: frame.copy(deep=True) for name, frame in (source_frames or {}).items()}
    valid_variants, invalid_variants = _source_backed_easyflow_variants(article_variants)

    family = assemblies.get("assembled_family", pd.Series("", index=assemblies.index)).fillna("").astype(str).str.lower()
    product_ids = assemblies.get("product_id", pd.Series("", index=assemblies.index)).fillna("").astype(str)
    easyflow = assemblies[(family.eq(EASYFLOW_FAMILY)) | product_ids.str.startswith("aco-assembled-easyflow-")].copy()

    results: list[AssemblyAttribution] = []
    for _, assembly in easyflow.iterrows():
        assembled_id = _text(assembly.get("product_id") or assembly.get("assembled_product_id"))
        detail_mask = details.get("assembled_product_id", pd.Series("", index=details.index)).fillna("").astype(str).eq(assembled_id)
        detail_rows = details[detail_mask].copy()
        detail = detail_rows.iloc[0] if not detail_rows.empty else pd.Series(dtype=object)
        parsed_base_id, parsed_component_id = excel_export._parse_final_set_parts(assembled_id, EASYFLOW_FAMILY)
        base_product_id = _text(detail.get("base_product_id")) or parsed_base_id
        component_id = _text(detail.get("component_id")) or parsed_component_id

        candidates = valid_variants.copy()
        if base_product_id and "base_product_id" in candidates.columns:
            candidates = candidates[candidates["base_product_id"].fillna("").astype(str).str.strip().eq(base_product_id)]
        candidate_articles = _article_numbers(candidates)
        explicit = _explicit_records(assembled_id, component_id, assembly, detail_rows, source_frames)
        explicit_articles = sorted({_normalized_article(row.get("article_number") or row.get("base_article_number")) for _, row, _ in explicit})
        explicit_articles = [article for article in explicit_articles if article]
        technical_matches, observed_fields = _technical_candidates(assembly, candidates)
        technical_articles = _article_numbers(technical_matches)
        conflicting_fields = _distinct_conflicting_fields(candidates)

        attributed = ""
        explicit_display = ", ".join(_article_display(article) for article in explicit_articles)
        method = "insufficient_article_level_evidence"
        confidence = "none"
        matched_url = ""
        matched_record = ""
        matched_fields: tuple[str, ...] = ()
        selected_row: pd.Series | None = None
        blocking_reason = ""

        if len(explicit_articles) > 1:
            method = "conflicting_article_level_evidence"
            blocking_reason = f"Conflicting exact-identity source records explicitly reference {explicit_display}."
        elif len(explicit_articles) == 1:
            article = explicit_articles[0]
            matching_explicit = [(name, row, index) for name, row, index in explicit if _record_article(row)[0] == article]
            candidate_article_rows = candidates[candidates["article_number"].map(_normalized_article).eq(article)]
            if candidate_article_rows.empty:
                method = "conflicting_article_level_evidence"
                blocking_reason = (
                    f"The exact-identity source record names {_article_display(article)}, but no valid same-article "
                    "technical source record exists in the candidate inventory."
                )
            else:
                source_name, source_row, source_index = matching_explicit[0]
                selected_row = source_row
                matched_fields, conflicts, _missing = _technical_comparison(assembly, source_row)
                explicit_signatures = {
                    tuple(_canonical_value(field, row.get(field)) for field in TECHNICAL_FIELDS)
                    for _name, row, _index in matching_explicit
                }
                if len(explicit_signatures) > 1 or conflicts:
                    method = "conflicting_article_level_evidence"
                    blocking_reason = (
                        f"Explicit article {_article_display(article)} has conflicting same-record technical evidence"
                        + (f" on {', '.join(conflicts)}" if conflicts else "")
                        + "."
                    )
                elif not matched_fields:
                    method = "invalid_source_record"
                    blocking_reason = "The explicit article record contains no matching technical evidence for the exact assembly."
                else:
                    method = "explicit_unique_article_match"
                    confidence = "high"
                    attributed = _article_display(article)
                    matched_url = _record_source_url(source_row)
                    matched_record = f"{source_name} row {source_index}: {_source_record_text(source_row, source_index)}"
        elif len(technical_articles) == 1:
            attributed = technical_articles[0]
            method = "unique_source_backed_technical_match"
            confidence = "medium"
            selected_rows = technical_matches[technical_matches["article_number"].map(_article_display).eq(attributed)]
            selected_row = selected_rows.iloc[0]
            matched_fields, _, _ = _technical_comparison(assembly, selected_row)
            matched_url = _record_source_url(selected_row)
            matched_record = _source_record_text(selected_row, selected_row.name)
        elif len(technical_articles) > 1:
            method = "ambiguous_multiple_article_candidates"
            blocking_reason = (
                f"The exact same-record assembly tuple matches multiple articles: {', '.join(technical_articles)}. "
                "Values from different article rows were not combined."
            )
        elif not candidates.empty:
            method = "insufficient_article_level_evidence"
            blocking_reason = (
                f"Article-specific records exist for {', '.join(candidate_articles)}, but the assembly provides no "
                f"unique exact technical tuple beyond {', '.join(observed_fields) or 'the family identity'}."
            )
        elif not invalid_variants.empty:
            method = "invalid_source_record"
            blocking_reason = "Easyflow candidate records exist, but required article identity, source URL, or evidence status is invalid."
        else:
            blocking_reason = "No valid article-specific Easyflow source record is available."

        found = method in {"explicit_unique_article_match", "unique_source_backed_technical_match"}
        ready = method == "explicit_unique_article_match"
        flow, height_min, height_max = _selected_values(selected_row if found else None)
        if found:
            next_action = (
                "Open a separate manual approval branch and verify the exact source record before any production change."
                if ready
                else "Obtain manual approval of the exact technical-tuple attribution before any production change."
            )
        else:
            next_action = (
                "Obtain an article-specific ACO technical table, catalogue row, datasheet, or product-page record "
                "that links the exact assembly/body identity, article number, and technical tuple."
            )

        results.append(
            AssemblyAttribution(
                assembled_product_id=assembled_id,
                product_family=EASYFLOW_FAMILY,
                base_product_id=base_product_id,
                selected_component_id=component_id,
                candidate_article_numbers=candidate_articles,
                explicit_article_number=explicit_display,
                attributed_article_number=attributed,
                attribution_method=method,
                attribution_confidence=confidence,
                matched_source_url=matched_url,
                matched_source_record=matched_record,
                matched_technical_fields=matched_fields,
                conflicting_technical_fields=conflicting_fields,
                flow_rate_lps=flow,
                height_adj_min_mm=height_min,
                height_adj_max_mm=height_max,
                article_level_attribution_found=found,
                ready_for_article_promotion_review=ready,
                blocking_reason=blocking_reason,
                recommended_next_action=next_action,
            )
        )

    counts = {
        "Products": 0,
        "Comparison": 0,
        "Components": 0,
        "BOM_Options": 0,
        "Final_Assemblies": len(assemblies),
        "Final_Set_Details": len(details),
        "Article_Variants": len(article_variants) if article_variants is not None else 0,
        "Easyflow_Assemblies_Reviewed": len(easyflow),
        "Easyflow_Candidate_Articles": len({_normalized_article(row.article_number) for row in _inventory(valid_variants)}),
        "Easyflow_Explicit_Unique_Matches": sum(row.attribution_method == "explicit_unique_article_match" for row in results),
        "Easyflow_Unique_Technical_Matches": sum(row.attribution_method == "unique_source_backed_technical_match" for row in results),
        "Easyflow_Blocked_Ambiguous_Assemblies": sum(not row.article_level_attribution_found for row in results),
        "Easyflow_Promotion_Review_Ready": sum(row.ready_for_article_promotion_review for row in results),
    }
    return AttributionReport(tuple(results), _inventory(valid_variants), counts)


def _read_optional_sheet(workbook: pd.ExcelFile, name: str) -> pd.DataFrame:
    if name not in workbook.sheet_names:
        return pd.DataFrame()
    return pd.read_excel(workbook, sheet_name=name)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def report_from_workbook(path: str | Path) -> AttributionReport:
    workbook_path = Path(path)
    before = _file_sha256(workbook_path)
    names = (
        "Products", "Comparison", "Components", "BOM_Options", "Final_Assemblies",
        "Final_Set_Details", "Article_Variants", "Evidence",
    )
    with pd.ExcelFile(workbook_path, engine="openpyxl") as workbook:
        sheets = {name: _read_optional_sheet(workbook, name) for name in names}
    report = assess_easyflow_article_attribution(
        sheets["Final_Assemblies"],
        sheets["Final_Set_Details"],
        sheets["Article_Variants"],
        source_frames={name: sheets[name] for name in ("Products", "Comparison", "Components", "Evidence")},
    )
    after = _file_sha256(workbook_path)
    counts = dict(report.sheet_counts)
    counts.update({name: len(sheets[name]) for name in ("Products", "Comparison", "Components", "BOM_Options")})
    return AttributionReport(report.assembly_results, report.source_evidence_inventory, counts, before == after)


def build_live_report() -> AttributionReport:
    frames = build_canonical_aco_frames(default_config())
    final_assemblies = excel_export._extract_final_assemblies(frames.products)
    final_set_details = excel_export._extract_final_set_details(final_assemblies, frames.bom_options, frames.excluded)
    article_variants = report_easyflow_article_variants.build_article_variants_dataframe(frames.registry, frames.products)
    report = assess_easyflow_article_attribution(
        final_assemblies,
        final_set_details,
        article_variants,
        source_frames={
            "Products": frames.products,
            "Comparison": frames.comparison,
            "Components": frames.excluded,
            "Evidence": frames.evidence,
        },
    )
    counts = dict(report.sheet_counts)
    counts.update({
        "Products": len(frames.products),
        "Comparison": len(frames.comparison),
        "Components": len(frames.excluded),
        "BOM_Options": len(frames.bom_options),
    })
    return AttributionReport(report.assembly_results, report.source_evidence_inventory, counts)


def _print_value(value: Any) -> str:
    if value in (None, "", (), []):
        return "empty"
    if isinstance(value, tuple):
        return ", ".join(str(item) for item in value) or "empty"
    return str(value)


def print_report(report: AttributionReport) -> None:
    print("ACO Easyflow second-stage article-level attribution diagnostic (read-only)")
    print("\nSummary:")
    for name, count in report.sheet_counts.items():
        print(f"- {name}: {count}")
    if report.workbook_byte_unchanged is not None:
        print(f"- workbook_byte_for_byte_unchanged: {report.workbook_byte_unchanged}")

    print("\nPer-assembly diagnostic frame:")
    for row in report.assembly_results:
        for field, value in asdict(row).items():
            print(f"- {field}: {_print_value(value)}" if field == "assembled_product_id" else f"  {field}: {_print_value(value)}")

    print("\nSource-evidence inventory:")
    for row in report.source_evidence_inventory:
        print(f"- article_number: {row.article_number} (original: {row.original_article_text})")
        for field, value in asdict(row).items():
            if field not in {"article_number", "original_article_text"}:
                print(f"  {field}: {_print_value(value)}")

    print(f"\nOVERALL: {report.overall_status}")
    print("- This diagnostic does not modify production frames, readiness flags, scoring, or workbook content.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report fail-closed Easyflow article-level attribution without changing production output.")
    parser.add_argument("--xlsx", type=Path, help="Read an existing canonical workbook instead of running live ACO discovery.")
    args = parser.parse_args(argv)
    report = report_from_workbook(args.xlsx) if args.xlsx else build_live_report()
    print_report(report)
    if args.xlsx and report.workbook_byte_unchanged is not True:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
