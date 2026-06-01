from __future__ import annotations

import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import _extract_final_assemblies, _extract_final_set_details
from tools import diagnose_mplus_base_row_sources as mplus_sources

ASSEMBLY_MODEL = "channel_body_x_drain_body_x_grate"
BLOCKING_REASON = "missing source-backed complete hydraulic fields for compound generation"
RECOMMENDED_NEXT_ACTION = (
    "find or parse source-backed flow_rate_lps and complete height attribution for M+ "
    "drain/channel combinations before production generation."
)
REQUIRED_TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)


@dataclass(frozen=True)
class ProposedCompoundMappingRow:
    channel_body_id: str
    channel_body_article_number: str
    drain_body_id: str
    drain_body_article_number: str
    grate_id: str
    grate_article_number: str
    proposed_compound_set_id: str
    proposed_product_name: str
    product_family: str
    assembly_model: str
    source_url_channel_body: str
    source_url_drain_body: str
    source_url_grate: str
    water_seal_mm: str
    outlet_dn: str
    flow_rate_lps: str
    height_adj_min_mm: str
    height_adj_max_mm: str
    missing_technical_fields: tuple[str, ...]
    mapping_confidence: str
    blocking_reason: str
    safe_to_generate: bool


@dataclass(frozen=True)
class RiskCheckSummary:
    duplicate_proposed_compound_set_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_channel_body_ids: int = 0
    missing_drain_body_ids: int = 0
    missing_grate_ids: int = 0
    missing_required_technical_fields: tuple[str, ...] = field(default_factory=tuple)
    optional_accessories_in_required_parts: tuple[str, ...] = field(default_factory=tuple)
    proposed_ids_overlapping_products: tuple[str, ...] = field(default_factory=tuple)
    proposed_ids_overlapping_final_assemblies: tuple[str, ...] = field(default_factory=tuple)
    proposed_ids_overlapping_final_set_details: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class MPlusCompoundMappingReport:
    sheet_counts: dict[str, int]
    raw_candidate_counts: dict[str, int]
    deduplicated_candidate_counts: dict[str, int]
    channel_bodies: tuple[mplus_sources.MPlusCandidateRow, ...]
    drain_bodies: tuple[mplus_sources.MPlusCandidateRow, ...]
    grates: tuple[mplus_sources.MPlusCandidateRow, ...]
    optional_accessories: tuple[mplus_sources.MPlusCandidateRow, ...]
    proposed_mappings: tuple[ProposedCompoundMappingRow, ...]
    mapping_confidence_summary: dict[str, int]
    safe_to_generate_counts: dict[str, int]
    missing_technical_field_summary: dict[str, int]
    risk_checks: RiskCheckSummary
    recommended_next_action: str = RECOMMENDED_NEXT_ACTION
    production_behavior_changed: bool = False


def _clean(value: Any) -> str:
    return mplus_sources._clean_text(value)


def _norm(df: pd.DataFrame | None, col: str) -> pd.Series:
    return mplus_sources._norm(df, col)


def _candidate_id(row: mplus_sources.MPlusCandidateRow) -> str:
    return _clean(row.proposed_product_id) or _clean(row.article_number) or _clean(row.source_url)


def _dedupe_candidates(
    rows: Iterable[mplus_sources.MPlusCandidateRow],
    *,
    role: str,
) -> tuple[mplus_sources.MPlusCandidateRow, ...]:
    seen: set[tuple[str, ...]] = set()
    deduped: list[mplus_sources.MPlusCandidateRow] = []
    for row in rows:
        article = _clean(row.article_number)
        proposed_id = _clean(row.proposed_product_id)
        source_url = mplus_sources._canonical_url(row.source_url)
        if role == "drain_body":
            key = (article or proposed_id or source_url,)
        else:
            key = (proposed_id or source_url, source_url if proposed_id else "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return tuple(deduped)


def _sheet_counts(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    components: pd.DataFrame,
    bom_options: pd.DataFrame,
    final_assemblies: pd.DataFrame,
    final_set_details: pd.DataFrame,
) -> dict[str, int]:
    return {
        "Products": len(products),
        "Comparison": len(comparison),
        "Candidates_All": len(candidates_all),
        "Components": len(components),
        "BOM_Options": len(bom_options),
        "Final_Assemblies": len(final_assemblies),
        "Final_Set_Details": len(final_set_details),
    }


def _field_from_drain_or_channel(
    field_name: str,
    drain: mplus_sources.MPlusCandidateRow,
    channel: mplus_sources.MPlusCandidateRow,
) -> str:
    drain_value = _clean(getattr(drain, field_name))
    if drain_value:
        return drain_value
    if field_name in {"height_adj_min_mm", "height_adj_max_mm"}:
        return _clean(getattr(channel, field_name))
    return ""


def _missing_fields(
    *,
    flow_rate_lps: str,
    water_seal_mm: str,
    outlet_dn: str,
    height_adj_min_mm: str,
    height_adj_max_mm: str,
) -> tuple[str, ...]:
    values = {
        "flow_rate_lps": flow_rate_lps,
        "water_seal_mm": water_seal_mm,
        "outlet_dn": outlet_dn,
        "height_adj_min_mm": height_adj_min_mm,
        "height_adj_max_mm": height_adj_max_mm,
    }
    return tuple(field for field in REQUIRED_TECHNICAL_FIELDS if not _clean(values[field]))


def _mapping_confidence(
    channel: mplus_sources.MPlusCandidateRow,
    drain: mplus_sources.MPlusCandidateRow,
    grate: mplus_sources.MPlusCandidateRow,
    missing: tuple[str, ...],
) -> str:
    if missing:
        return "blocked_source_backed_parts_incomplete_technical_fields"
    if all(row.confidence == "high" for row in (channel, drain, grate)):
        return "high_source_backed_parts_complete_fields"
    return "medium_source_backed_parts_complete_fields"


def _proposed_product_name(
    channel: mplus_sources.MPlusCandidateRow,
    drain: mplus_sources.MPlusCandidateRow,
    grate: mplus_sources.MPlusCandidateRow,
) -> str:
    return (
        "Diagnostic ACO ShowerDrain M+ compound assembly "
        f"channel {channel.article_number or _candidate_id(channel)} + "
        f"drain {drain.article_number or _candidate_id(drain)} + "
        f"grate {grate.article_number or _candidate_id(grate)}"
    )


def build_proposed_mappings(
    channel_bodies: Iterable[mplus_sources.MPlusCandidateRow],
    drain_bodies: Iterable[mplus_sources.MPlusCandidateRow],
    grates: Iterable[mplus_sources.MPlusCandidateRow],
) -> tuple[ProposedCompoundMappingRow, ...]:
    rows: list[ProposedCompoundMappingRow] = []
    for channel in channel_bodies:
        for drain in drain_bodies:
            for grate in grates:
                channel_id = _candidate_id(channel)
                drain_id = _candidate_id(drain)
                grate_id = _candidate_id(grate)
                water_seal_mm = _field_from_drain_or_channel("water_seal_mm", drain, channel)
                outlet_dn = _field_from_drain_or_channel("outlet_dn", drain, channel)
                flow_rate_lps = _field_from_drain_or_channel("flow_rate_lps", drain, channel)
                height_adj_min_mm = _field_from_drain_or_channel("height_adj_min_mm", drain, channel)
                height_adj_max_mm = _field_from_drain_or_channel("height_adj_max_mm", drain, channel)
                missing = _missing_fields(
                    flow_rate_lps=flow_rate_lps,
                    water_seal_mm=water_seal_mm,
                    outlet_dn=outlet_dn,
                    height_adj_min_mm=height_adj_min_mm,
                    height_adj_max_mm=height_adj_max_mm,
                )
                classes_exist = bool(channel_id and drain_id and grate_id)
                source_backed_relationship = all(
                    row.product_family == mplus_sources.MPLUS_FAMILY and row.system_role
                    for row in (channel, drain, grate)
                )
                safe_to_generate = bool(classes_exist and source_backed_relationship and not missing)
                rows.append(
                    ProposedCompoundMappingRow(
                        channel_body_id=channel_id,
                        channel_body_article_number=_clean(channel.article_number),
                        drain_body_id=drain_id,
                        drain_body_article_number=_clean(drain.article_number),
                        grate_id=grate_id,
                        grate_article_number=_clean(grate.article_number),
                        proposed_compound_set_id=f"diagnostic-mplus-{channel_id}__{drain_id}__{grate_id}",
                        proposed_product_name=_proposed_product_name(channel, drain, grate),
                        product_family=mplus_sources.MPLUS_FAMILY,
                        assembly_model=ASSEMBLY_MODEL,
                        source_url_channel_body=mplus_sources._canonical_url(channel.source_url),
                        source_url_drain_body=mplus_sources._canonical_url(drain.source_url),
                        source_url_grate=mplus_sources._canonical_url(grate.source_url),
                        water_seal_mm=water_seal_mm,
                        outlet_dn=outlet_dn,
                        flow_rate_lps=flow_rate_lps,
                        height_adj_min_mm=height_adj_min_mm,
                        height_adj_max_mm=height_adj_max_mm,
                        missing_technical_fields=missing,
                        mapping_confidence=_mapping_confidence(channel, drain, grate, missing),
                        blocking_reason="" if safe_to_generate else BLOCKING_REASON,
                        safe_to_generate=safe_to_generate,
                    )
                )
    return tuple(rows)


def _id_set(df: pd.DataFrame | None, *columns: str) -> set[str]:
    df = pd.DataFrame() if df is None else df
    values: set[str] = set()
    for col in columns:
        if col in df.columns:
            values.update(_clean(value) for value in df[col].fillna("").astype(str))
    return {value for value in values if value}


def _risk_checks(
    mappings: tuple[ProposedCompoundMappingRow, ...],
    optional_accessories: tuple[mplus_sources.MPlusCandidateRow, ...],
    products: pd.DataFrame,
    final_assemblies: pd.DataFrame,
    final_set_details: pd.DataFrame,
) -> RiskCheckSummary:
    proposed_ids = [row.proposed_compound_set_id for row in mappings]
    duplicate_ids = tuple(sorted(item for item, count in Counter(proposed_ids).items() if count > 1))
    missing_fields = tuple(sorted({field for row in mappings for field in row.missing_technical_fields}))
    accessory_ids = {_candidate_id(row) for row in optional_accessories}
    required_part_ids = {
        part_id
        for row in mappings
        for part_id in (row.channel_body_id, row.drain_body_id, row.grate_id)
        if part_id
    }
    proposed_id_set = set(proposed_ids)
    return RiskCheckSummary(
        duplicate_proposed_compound_set_ids=duplicate_ids,
        missing_channel_body_ids=sum(1 for row in mappings if not row.channel_body_id),
        missing_drain_body_ids=sum(1 for row in mappings if not row.drain_body_id),
        missing_grate_ids=sum(1 for row in mappings if not row.grate_id),
        missing_required_technical_fields=missing_fields,
        optional_accessories_in_required_parts=tuple(sorted(accessory_ids & required_part_ids)),
        proposed_ids_overlapping_products=tuple(sorted(proposed_id_set & _id_set(products, "product_id"))),
        proposed_ids_overlapping_final_assemblies=tuple(sorted(proposed_id_set & _id_set(final_assemblies, "product_id"))),
        proposed_ids_overlapping_final_set_details=tuple(
            sorted(proposed_id_set & _id_set(final_set_details, "set_id", "assembled_product_id"))
        ),
    )


def build_report(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    components: pd.DataFrame,
    bom_options: pd.DataFrame,
    final_assemblies: pd.DataFrame | None = None,
    final_set_details: pd.DataFrame | None = None,
) -> MPlusCompoundMappingReport:
    final_assemblies = pd.DataFrame() if final_assemblies is None else final_assemblies
    final_set_details = pd.DataFrame() if final_set_details is None else final_set_details
    diag = mplus_sources.build_diagnostic(
        products,
        comparison,
        candidates_all,
        components,
        bom_options,
        final_assemblies,
        final_set_details,
    )
    channel_raw = diag.channel_body_candidates
    drain_raw = diag.drain_body_candidates
    grate_raw = diag.grate_candidates
    accessories_raw = diag.accessory_candidates
    channel_bodies = _dedupe_candidates(channel_raw, role="channel_body")
    drain_bodies = _dedupe_candidates(drain_raw, role="drain_body")
    grates = _dedupe_candidates(grate_raw, role="grate")
    optional_accessories = _dedupe_candidates(accessories_raw, role="accessory")
    mappings = build_proposed_mappings(channel_bodies, drain_bodies, grates)
    confidence_summary = dict(sorted(Counter(row.mapping_confidence for row in mappings).items()))
    safe_counter = Counter("safe" if row.safe_to_generate else "blocked" for row in mappings)
    missing_summary = dict(sorted(Counter(field for row in mappings for field in row.missing_technical_fields).items()))

    return MPlusCompoundMappingReport(
        sheet_counts=_sheet_counts(
            candidates_all,
            products,
            comparison,
            components,
            bom_options,
            final_assemblies,
            final_set_details,
        ),
        raw_candidate_counts={
            "channel_bodies": len(channel_raw),
            "drain_bodies": len(drain_raw),
            "grates": len(grate_raw),
            "optional_accessories": len(accessories_raw),
        },
        deduplicated_candidate_counts={
            "channel_bodies": len(channel_bodies),
            "drain_bodies": len(drain_bodies),
            "grates": len(grates),
            "optional_accessories": len(optional_accessories),
        },
        channel_bodies=channel_bodies,
        drain_bodies=drain_bodies,
        grates=grates,
        optional_accessories=optional_accessories,
        proposed_mappings=mappings,
        mapping_confidence_summary=confidence_summary,
        safe_to_generate_counts={"safe": safe_counter.get("safe", 0), "blocked": safe_counter.get("blocked", 0)},
        missing_technical_field_summary=missing_summary,
        risk_checks=_risk_checks(mappings, optional_accessories, products, final_assemblies, final_set_details),
    )


def _print_count_dict(title: str, values: dict[str, int]) -> None:
    print(f"\n{title}:")
    for key, value in values.items():
        print(f"- {key}: {value}")


def print_report(report: MPlusCompoundMappingReport) -> None:
    print("ACO ShowerDrain M+ compound assembly mapping diagnostic (proposal-only; no generation)")
    _print_count_dict("Input sheet/frame counts", report.sheet_counts)
    _print_count_dict("M+ raw candidate counts by class", report.raw_candidate_counts)
    _print_count_dict("M+ deduplicated candidate counts by class", report.deduplicated_candidate_counts)

    print(f"\nProposed compound mapping count: {len(report.proposed_mappings)}")
    print("\nProposed diagnostic set IDs:")
    for row in report.proposed_mappings:
        print(f"- {row.proposed_compound_set_id}")

    print("\nProposed compound mapping rows:")
    for row in report.proposed_mappings:
        print(
            "- "
            f"set_id={row.proposed_compound_set_id}; "
            f"channel_body_id={row.channel_body_id}; channel_article={row.channel_body_article_number or '(missing)'}; "
            f"drain_body_id={row.drain_body_id}; drain_article={row.drain_body_article_number or '(missing)'}; "
            f"grate_id={row.grate_id}; grate_article={row.grate_article_number or '(missing)'}; "
            f"water_seal_mm={row.water_seal_mm or '(missing)'}; outlet_dn={row.outlet_dn or '(missing)'}; "
            f"flow_rate_lps={row.flow_rate_lps or '(missing)'}; "
            f"height_adj_min_mm={row.height_adj_min_mm or '(missing)'}; "
            f"height_adj_max_mm={row.height_adj_max_mm or '(missing)'}; "
            f"missing_technical_fields={','.join(row.missing_technical_fields) or '(none)'}; "
            f"mapping_confidence={row.mapping_confidence}; safe_to_generate={row.safe_to_generate}; "
            f"blocking_reason={row.blocking_reason or '(none)'}"
        )

    _print_count_dict("Mapping confidence summary", report.mapping_confidence_summary)
    _print_count_dict("safe_to_generate counts", report.safe_to_generate_counts)
    _print_count_dict("Missing technical-field summary", report.missing_technical_field_summary)
    print(f"\nOptional accessory count: {len(report.optional_accessories)}")

    r = report.risk_checks
    print("\nRisk checks:")
    print(f"- duplicate proposed compound set IDs: {', '.join(r.duplicate_proposed_compound_set_ids) or 'none'}")
    print(f"- missing channel body ID: {r.missing_channel_body_ids}")
    print(f"- missing drain body ID: {r.missing_drain_body_ids}")
    print(f"- missing grate ID: {r.missing_grate_ids}")
    print(f"- missing required technical fields: {', '.join(r.missing_required_technical_fields) or 'none'}")
    print(f"- optional accessories incorrectly treated as required parts: {', '.join(r.optional_accessories_in_required_parts) or 'none'}")
    print(f"- proposed ID overlap with Products.product_id: {', '.join(r.proposed_ids_overlapping_products) or 'none'}")
    print(
        "- proposed ID overlap with Final_Assemblies.product_id: "
        f"{', '.join(r.proposed_ids_overlapping_final_assemblies) or 'none'}"
    )
    print(
        "- proposed ID overlap with Final_Set_Details set/assembled IDs: "
        f"{', '.join(r.proposed_ids_overlapping_final_set_details) or 'none'}"
    )
    print(f"\nRecommended next action: {report.recommended_next_action}")
    print("production behavior changed: no")


def main() -> int:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(registry_rows)
    products, comparison, components, _evidence, bom_options = pipeline.run_update(candidates_all, default_config())
    final_assemblies = _extract_final_assemblies(products)
    final_set_details = _extract_final_set_details(final_assemblies, bom_options, components)
    report = build_report(candidates_all, products, comparison, components, bom_options, final_assemblies, final_set_details)
    print_report(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
