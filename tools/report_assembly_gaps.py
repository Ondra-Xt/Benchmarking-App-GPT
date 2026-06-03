from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src import excel_export

ASSEMBLED_PREFIX = "aco-assembled-"
REQUIRED_BASE_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
ACTIVE_FAMILIES = (
    "showerdrain_splus",
    "showerdrain_c",
    "easyflow",
    "easyflowplus",
)
REQUIRED_BLOCKED_FAMILIES = (
    "showerdrain_cplus",
    "showerdrain_mplus",
    "showerdrain_eplus",
    "showerdrain_b",
)
STATUS_ACTIONS = {
    "already_active": "no action; existing assembled output is baseline-protected",
    "ready_candidate": "review proposed count, then implement source-backed assembly generation in a separate production patch",
    "blocked_no_base_rows": "find base product source",
    "blocked_no_compatible_grate_evidence": "find explicit C+ compatible grate evidence / article matrix",
    "blocked_no_valid_components": "add component-only candidate; parse article table if component IDs are only present in source text",
    "blocked_incomplete_hydraulic_data": "find base product source with complete hydraulic data",
    "blocked_article_variant_ambiguous": "keep blocked because ambiguity remains; parse article table and resolve unique variant attribution",
    "blocked_proposal_only_flow_policy": "accept benchmark policy for multi-head-condition flow values before generating M+ production assemblies",
}


@dataclass(frozen=True)
class FamilyGap:
    family: str
    status: str
    current_assembled_count: int
    base_product_rows_found: int
    hydraulic_complete_base_rows_found: int
    compatible_grate_bom_rows_found: int
    valid_grate_component_rows_found: int
    optional_accessory_rows_found: int
    missing_component_ids: tuple[str, ...] = field(default_factory=tuple)
    dangling_component_ids: tuple[str, ...] = field(default_factory=tuple)
    source_urls_available: tuple[str, ...] = field(default_factory=tuple)
    proposed_assembled_product_count: int = 0
    next_required_action: str = ""
    proposal_only_mapping_count: int = 0
    proposal_assembly_model: str = ""
    proposal_safe_to_generate_count: int = 0
    proposal_blocked_count: int = 0
    proposal_flow_policy: str = ""
    proposal_flow_rate_lps_10mm_head: str = ""
    proposal_flow_rate_lps_20mm_head: str = ""
    proposal_selected_default_flow_rate_lps: str = ""
    proposal_blocking_reason: str = ""


@dataclass(frozen=True)
class ProposalOnlyMappingSummary:
    family: str
    mapping_count: int
    assembly_model: str
    safe_to_generate_count: int
    blocked_count: int
    flow_policy: str
    flow_rate_lps_10mm_head: str
    flow_rate_lps_20mm_head: str
    selected_default_flow_rate_lps: str
    blocking_reason: str


@dataclass(frozen=True)
class AssemblyGapReport:
    sheet_counts: dict[str, int]
    current_assembled_counts: dict[str, int]
    families: tuple[FamilyGap, ...]
    ready_candidate_families: tuple[str, ...]
    proposal_only_mapping_families: tuple[str, ...]
    proposal_only_mappings: tuple[ProposalOnlyMappingSummary, ...]
    missing_components: dict[str, tuple[str, ...]]
    dangling_components: dict[str, tuple[str, ...]]


def _norm(df: pd.DataFrame | None, col: str) -> pd.Series:
    df = pd.DataFrame() if df is None else df
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str).str.strip()


def _nonempty(value: Any) -> bool:
    return not (pd.isna(value) or str(value).strip() == "")


def _canonical_family(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _assembled_family(product_id: str, row_family: str = "") -> str:
    if row_family:
        return _canonical_family(row_family)
    pid = str(product_id or "")
    if not pid.startswith(ASSEMBLED_PREFIX):
        return ""
    tail = pid[len(ASSEMBLED_PREFIX):]
    known = {
        "showerdrain-splus-": "showerdrain_splus",
        "showerdrain-cplus-": "showerdrain_cplus",
        "showerdrain-mplus-": "showerdrain_mplus",
        "showerdrain-eplus-": "showerdrain_eplus",
        "showerdrain-c-": "showerdrain_c",
        "showerdrain-b-": "showerdrain_b",
        "easyflowplus-": "easyflowplus",
        "easyflow-": "easyflow",
    }
    for prefix, family in known.items():
        if tail.startswith(prefix):
            return family
    return _canonical_family(tail.split("__", 1)[0].rsplit("-", 1)[0])


def _families_from_frame(df: pd.DataFrame, *columns: str) -> set[str]:
    families: set[str] = set()
    for col in columns:
        families.update(_canonical_family(value) for value in _norm(df, col) if value)
    return {family for family in families if family}


def _urls_from_row(row: pd.Series) -> set[str]:
    urls: set[str] = set()
    for col in ("product_url", "source_url", "sources", "url"):
        raw = str(row.get(col, "") or "")
        for token in raw.replace(";", " ").replace(",", " ").split():
            if token.startswith("http://") or token.startswith("https://"):
                urls.add(token.strip())
    return urls


def _source_urls_for_family(family: str, frames: Iterable[pd.DataFrame]) -> tuple[str, ...]:
    urls: set[str] = set()
    for df in frames:
        if df is None or df.empty:
            continue
        fam_cols = [col for col in ("product_family", "parent_family", "option_family", "assembled_family") if col in df.columns]
        if not fam_cols:
            continue
        mask = pd.Series(False, index=df.index)
        for col in fam_cols:
            mask = mask | _norm(df, col).map(_canonical_family).eq(family)
        for _, row in df[mask].iterrows():
            urls.update(_urls_from_row(row))
    return tuple(sorted(urls))


def _hydraulic_complete(row: pd.Series) -> bool:
    return all(_nonempty(row.get(field, "")) for field in REQUIRED_BASE_FIELDS)


def _article_variant_ambiguous(family: str, article_variants: pd.DataFrame) -> bool:
    if article_variants is None or article_variants.empty:
        return False
    fam_rows = article_variants[_norm(article_variants, "product_family").map(_canonical_family).eq(family)]
    if fam_rows.empty:
        return False
    statuses = _norm(fam_rows, "attribution_status").str.lower()
    reasons = _norm(fam_rows, "why_not_promoted").str.lower()
    candidate_like = _norm(fam_rows, "variant_type").str.lower().str.contains("candidate", regex=False)
    ambiguous = statuses.str.contains("multiple|ambiguous|pending", regex=True) | reasons.str.contains("ambiguous|pending", regex=True)
    return bool((candidate_like & ambiguous).any())




def _bool_series(series: pd.Series) -> pd.Series:
    truthy = {"true", "1", "yes", "y"}
    return series.fillna(False).astype(str).str.strip().str.lower().isin(truthy)


def _unique_join(values: pd.Series) -> str:
    unique = tuple(dict.fromkeys(value for value in values.fillna("").astype(str).str.strip() if value))
    return ", ".join(unique)


def _proposal_mapping_summaries(mplus_compound_mappings: pd.DataFrame) -> tuple[ProposalOnlyMappingSummary, ...]:
    if mplus_compound_mappings is None or mplus_compound_mappings.empty:
        return ()

    families = _norm(mplus_compound_mappings, "product_family").map(_canonical_family)
    summaries: list[ProposalOnlyMappingSummary] = []
    for family in sorted(family for family in set(families) if family):
        rows = mplus_compound_mappings[families.eq(family)]
        safe = _bool_series(rows.get("safe_to_generate", pd.Series(False, index=rows.index)))
        mapping_count = int(len(rows))
        safe_count = int(safe.sum())
        summaries.append(
            ProposalOnlyMappingSummary(
                family=family,
                mapping_count=mapping_count,
                assembly_model=_unique_join(_norm(rows, "assembly_model")),
                safe_to_generate_count=safe_count,
                blocked_count=mapping_count - safe_count,
                flow_policy=_unique_join(_norm(rows, "flow_policy")),
                flow_rate_lps_10mm_head=_unique_join(_norm(rows, "flow_rate_lps_10mm_head")),
                flow_rate_lps_20mm_head=_unique_join(_norm(rows, "flow_rate_lps_20mm_head")),
                selected_default_flow_rate_lps=_unique_join(_norm(rows, "selected_default_flow_rate_lps")),
                blocking_reason=_unique_join(_norm(rows, "blocking_reason")),
            )
        )
    return tuple(summaries)

def _component_families(components: pd.DataFrame) -> pd.Series:
    option = _norm(components, "option_family").map(_canonical_family)
    product = _norm(components, "product_family").map(_canonical_family)
    return option.where(option != "", product)


def _family_gap(
    family: str,
    products: pd.DataFrame,
    components: pd.DataFrame,
    bom: pd.DataFrame,
    article_variants: pd.DataFrame,
    source_frames: tuple[pd.DataFrame, ...],
    proposal_mapping: ProposalOnlyMappingSummary | None = None,
) -> FamilyGap:
    product_ids = _norm(products, "product_id")
    product_families = _norm(products, "product_family").map(_canonical_family)
    assembled_mask = product_ids.str.startswith(ASSEMBLED_PREFIX)
    assembled_families = pd.Series(
        [_assembled_family(pid, fam) for pid, fam in zip(product_ids, product_families)],
        index=products.index,
        dtype="string",
    )
    current = int((assembled_mask & assembled_families.eq(family)).sum())

    base_mask = (~assembled_mask) & product_families.eq(family)
    bases = products[base_mask].copy()
    complete_bases = bases[bases.apply(_hydraulic_complete, axis=1)] if not bases.empty else bases

    bom_type = _norm(bom, "option_type").str.lower()
    parent_family = _norm(bom, "parent_family").map(_canonical_family)
    bom_product_ids = _norm(bom, "product_id")

    family_bom_mask = parent_family.eq(family)
    if not bases.empty:
        family_bom_mask = family_bom_mask & bom_product_ids.isin(set(_norm(bases, "product_id")))
    compatible = bom[family_bom_mask & bom_type.eq("compatible_grate")].copy()
    optional_accessory_count = int((family_bom_mask & bom_type.eq("optional_accessory")).sum())

    component_ids = set(_norm(components, "product_id")) - {""}
    component_families = _component_families(components)
    family_component_ids = set(_norm(components[component_families.eq(family)], "product_id")) - {""}
    compatible_component_ids = set(_norm(compatible, "component_id")) - {""}
    missing_ids = tuple(sorted(cid for cid in compatible_component_ids if cid not in component_ids))
    valid_ids = tuple(sorted(cid for cid in compatible_component_ids if cid in component_ids))
    valid_compatible_rows = compatible[_norm(compatible, "component_id").isin(set(valid_ids))]
    dangling_ids = tuple(sorted(cid for cid in family_component_ids if cid not in compatible_component_ids))

    proposed = 0
    if len(complete_bases) > 0 and valid_ids:
        complete_base_ids = set(_norm(complete_bases, "product_id"))
        proposed = int(_norm(valid_compatible_rows, "product_id").isin(complete_base_ids).sum())

    if current > 0:
        status = "already_active"
    elif proposal_mapping is not None and proposal_mapping.blocked_count > 0:
        status = "blocked_proposal_only_flow_policy"
    elif len(bases) == 0:
        status = "blocked_no_base_rows"
    elif len(complete_bases) == 0:
        status = "blocked_incomplete_hydraulic_data"
    elif len(compatible) == 0:
        status = "blocked_no_compatible_grate_evidence"
    elif not valid_ids:
        status = "blocked_no_valid_components"
    elif _article_variant_ambiguous(family, article_variants):
        status = "blocked_article_variant_ambiguous"
    else:
        status = "ready_candidate"

    return FamilyGap(
        family=family,
        status=status,
        current_assembled_count=current,
        base_product_rows_found=int(len(bases)),
        hydraulic_complete_base_rows_found=int(len(complete_bases)),
        compatible_grate_bom_rows_found=int(len(compatible)),
        valid_grate_component_rows_found=int(len(valid_compatible_rows)),
        optional_accessory_rows_found=optional_accessory_count,
        missing_component_ids=missing_ids,
        dangling_component_ids=dangling_ids,
        source_urls_available=_source_urls_for_family(family, source_frames),
        proposed_assembled_product_count=proposed if status == "ready_candidate" else 0,
        next_required_action=STATUS_ACTIONS[status],
        proposal_only_mapping_count=proposal_mapping.mapping_count if proposal_mapping else 0,
        proposal_assembly_model=proposal_mapping.assembly_model if proposal_mapping else "",
        proposal_safe_to_generate_count=proposal_mapping.safe_to_generate_count if proposal_mapping else 0,
        proposal_blocked_count=proposal_mapping.blocked_count if proposal_mapping else 0,
        proposal_flow_policy=proposal_mapping.flow_policy if proposal_mapping else "",
        proposal_flow_rate_lps_10mm_head=proposal_mapping.flow_rate_lps_10mm_head if proposal_mapping else "",
        proposal_flow_rate_lps_20mm_head=proposal_mapping.flow_rate_lps_20mm_head if proposal_mapping else "",
        proposal_selected_default_flow_rate_lps=proposal_mapping.selected_default_flow_rate_lps if proposal_mapping else "",
        proposal_blocking_reason=proposal_mapping.blocking_reason if proposal_mapping else "",
    )


def build_report(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    components: pd.DataFrame,
    bom: pd.DataFrame,
    final_assemblies: pd.DataFrame | None = None,
    final_set_details: pd.DataFrame | None = None,
    article_variants: pd.DataFrame | None = None,
    mplus_compound_mappings: pd.DataFrame | None = None,
) -> AssemblyGapReport:
    candidates_all = pd.DataFrame() if candidates_all is None else candidates_all.copy()
    products = pd.DataFrame() if products is None else products.copy()
    components = pd.DataFrame() if components is None else components.copy()
    bom = pd.DataFrame() if bom is None else bom.copy()
    final_assemblies = pd.DataFrame() if final_assemblies is None else final_assemblies.copy()
    final_set_details = pd.DataFrame() if final_set_details is None else final_set_details.copy()
    article_variants = pd.DataFrame() if article_variants is None else article_variants.copy()
    mplus_compound_mappings = pd.DataFrame() if mplus_compound_mappings is None else mplus_compound_mappings.copy()
    proposal_mappings = _proposal_mapping_summaries(mplus_compound_mappings)
    proposal_by_family = {summary.family: summary for summary in proposal_mappings}

    if final_assemblies.empty:
        final_assemblies = excel_export._extract_final_assemblies(products)
    if final_set_details.empty:
        final_set_details = excel_export._extract_final_set_details(final_assemblies, bom, components)
    if article_variants.empty:
        try:
            article_variants = excel_export._extract_article_variants(candidates_all, products)
        except Exception:
            article_variants = pd.DataFrame()

    families = set(ACTIVE_FAMILIES) | set(REQUIRED_BLOCKED_FAMILIES)
    families |= _families_from_frame(candidates_all, "product_family", "option_family")
    families |= _families_from_frame(products, "product_family")
    families |= _families_from_frame(components, "product_family", "option_family")
    families |= _families_from_frame(bom, "parent_family", "option_family")
    families |= _families_from_frame(final_assemblies, "assembled_family", "product_family")
    families |= _families_from_frame(final_set_details, "assembled_family", "component_family")
    families |= set(proposal_by_family)

    ordered = [*ACTIVE_FAMILIES, *REQUIRED_BLOCKED_FAMILIES]
    ordered.extend(sorted(f for f in families if f not in set(ordered)))

    gaps = tuple(
        _family_gap(
            family,
            products,
            components,
            bom,
            article_variants,
            (candidates_all, products, components, bom, final_assemblies, final_set_details, article_variants, mplus_compound_mappings),
            proposal_by_family.get(family),
        )
        for family in ordered
        if family
    )
    current_counts = {family: 0 for family in ACTIVE_FAMILIES}
    for gap in gaps:
        if gap.family in current_counts:
            current_counts[gap.family] = gap.current_assembled_count

    return AssemblyGapReport(
        sheet_counts={
            "Products": len(products),
            "Candidates_All": len(candidates_all),
            "Components": len(components),
            "BOM_Options": len(bom),
            "Final_Assemblies": len(final_assemblies),
            "Final_Set_Details": len(final_set_details),
            "Mplus_Compound_Mappings": len(mplus_compound_mappings),
            "Article_Variants": len(article_variants),
        },
        current_assembled_counts=current_counts,
        families=gaps,
        ready_candidate_families=tuple(g.family for g in gaps if g.status == "ready_candidate"),
        proposal_only_mapping_families=tuple(summary.family for summary in proposal_mappings),
        proposal_only_mappings=proposal_mappings,
        missing_components={g.family: g.missing_component_ids for g in gaps if g.missing_component_ids},
        dangling_components={g.family: g.dangling_component_ids for g in gaps if g.dangling_component_ids},
    )


def print_report(report: AssemblyGapReport) -> None:
    print("ACO assembly gap diagnostic (diagnostic-only; no XLSX write)")
    print("\nSheet counts inspected:")
    for name, count in report.sheet_counts.items():
        print(f"- {name}: {count}")

    print("\nCurrent assembled family counts:")
    for family in ACTIVE_FAMILIES:
        print(f"- {family}: {report.current_assembled_counts.get(family, 0)}")

    print("\nFamily gap table:")
    header = "family | assembled | base | hydraulic_complete | compatible_grate_bom | valid_grates | optional_accessories | status | proposed | proposal_only_mappings | next_required_action"
    print(header)
    print("-" * len(header))
    for gap in report.families:
        print(
            f"{gap.family} | {gap.current_assembled_count} | {gap.base_product_rows_found} | "
            f"{gap.hydraulic_complete_base_rows_found} | {gap.compatible_grate_bom_rows_found} | "
            f"{gap.valid_grate_component_rows_found} | {gap.optional_accessory_rows_found} | "
            f"{gap.status} | {gap.proposed_assembled_product_count} | "
            f"{gap.proposal_only_mapping_count} | {gap.next_required_action}"
        )

    print("\nReady candidate families:")
    if report.ready_candidate_families:
        for family in report.ready_candidate_families:
            gap = next(g for g in report.families if g.family == family)
            print(f"- {family}: proposed assembled product count = {gap.proposed_assembled_product_count}")
    else:
        print("- none")

    print("\nProposal-only diagnostic mappings:")
    if report.proposal_only_mappings:
        for summary in report.proposal_only_mappings:
            selected = summary.selected_default_flow_rate_lps or "empty"
            print(
                f"- {summary.family}: {summary.mapping_count} mappings, "
                f"safe_to_generate={summary.safe_to_generate_count}, "
                f"blocked={summary.blocked_count}, reason={summary.blocking_reason or 'none'}"
            )
            print(f"  assembly_model={summary.assembly_model or 'none'}")
            print(f"  flow_policy={summary.flow_policy or 'none'}")
            print(f"  flow_rate_lps_10mm_head={summary.flow_rate_lps_10mm_head or 'empty'}")
            print(f"  flow_rate_lps_20mm_head={summary.flow_rate_lps_20mm_head or 'empty'}")
            print(f"  selected_default_flow_rate_lps={selected}")
    else:
        print("- none")

    print("\nMissing component IDs by family:")
    if report.missing_components:
        for family, ids in report.missing_components.items():
            print(f"- {family}: {', '.join(ids)}")
    else:
        print("- none")

    print("\nDangling component IDs by family:")
    if report.dangling_components:
        for family, ids in report.dangling_components.items():
            print(f"- {family}: {', '.join(ids)}")
    else:
        print("- none")

    print("\nSource URLs available by family:")
    for gap in report.families:
        suffix = ", ".join(gap.source_urls_available) if gap.source_urls_available else "none"
        print(f"- {gap.family}: {suffix}")

    print("\nRecommended next implementation target:")
    if report.ready_candidate_families:
        print(f"- {report.ready_candidate_families[0]} (ready_candidate; implement in a separate production patch)")
    else:
        blocked = next((g for g in report.families if g.status != "already_active"), None)
        if blocked is None:
            print("- none")
        else:
            print(f"- {blocked.family}: {blocked.next_required_action}")

    print("\nProduction behavior changed: no (report only; no product, assembly, BOM, scoring, connector, validator, or XLSX export changes)")


def main() -> int:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, components, _evidence, bom = pipeline.run_update(registry, default_config())
    final_assemblies = excel_export._extract_final_assemblies(products)
    final_set_details = excel_export._extract_final_set_details(final_assemblies, bom, components)
    mplus_compound_mappings = excel_export._extract_mplus_compound_mappings(
        registry,
        products,
        comparison,
        components,
        bom,
        final_assemblies,
        final_set_details,
    )
    report = build_report(
        registry,
        products,
        components,
        bom,
        final_assemblies=final_assemblies,
        final_set_details=final_set_details,
        mplus_compound_mappings=mplus_compound_mappings,
    )
    print_report(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
