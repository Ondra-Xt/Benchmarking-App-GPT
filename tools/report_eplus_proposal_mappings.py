from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import _extract_final_assemblies, _extract_final_set_details
from tools import diagnose_eplus_base_row_sources as eplus_sources

PRODUCT_FAMILY = "showerdrain_eplus"
ASSEMBLY_MODEL = "base_x_grate"
COMPATIBILITY_EVIDENCE_TYPE = "no_explicit_article_level_matrix_found"
COMPATIBILITY_CONFIDENCE = "low"
DATA_QUALITY_STATUS = "proposal_only_partial"
MISSING_EVIDENCE = "explicit_article_level_base_to_grate_compatibility"
BLOCKING_REASON = (
    "E+ diagnostic has only conservative page-level body/grate evidence and no explicit "
    "article-level base-to-grate compatibility matrix."
)
RECOMMENDED_NEXT_ACTION = (
    "collect explicit article-level E+ base-to-grate compatibility before production generation."
)
PRODUCTION_STATUS_NOTE = "diagnostic/proposal-only; no Products/BOM/assembly generation change."
PRODUCTION_BEHAVIOR_CHANGED = False
REQUIRED_HYDRAULIC_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
PROPOSAL_COLUMNS = (
    "set_id",
    "product_family",
    "assembly_model",
    "body_id",
    "body_article_number",
    "body_source_url",
    "grate_id",
    "grate_article_number",
    "grate_source_url",
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "body_evidence_type",
    "body_confidence",
    "grate_evidence_type",
    "grate_confidence",
    "compatibility_evidence_type",
    "compatibility_confidence",
    "article_level_compatibility_found",
    "data_quality_status",
    "missing_evidence",
    "safe_to_generate",
    "ready_for_benchmark",
    "ready_for_customer_view",
    "blocking_reason",
    "recommended_next_action",
    "production_status_note",
)


@dataclass(frozen=True)
class RiskCheck:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class EPlusProposalReport:
    input_frame_counts: dict[str, int]
    diagnostic: eplus_sources.EPlusDiagnostic
    proposed_mappings: pd.DataFrame
    risk_checks: tuple[RiskCheck, ...]
    proposed_mapping_count: int
    safe_to_generate_count: int
    blocked_count: int
    production_ready: bool
    blocking_reason: str
    recommended_next_action: str
    production_behavior_changed: bool = False


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _norm(df: pd.DataFrame | None, column: str) -> pd.Series:
    df = pd.DataFrame() if df is None else df
    if column not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[column].fillna("").astype(str).str.strip()


def _ids_in(df: pd.DataFrame | None, columns: Iterable[str], proposed_ids: set[str]) -> tuple[str, ...]:
    df = pd.DataFrame() if df is None else df
    found: set[str] = set()
    for column in columns:
        if column in df.columns:
            found.update(set(_norm(df, column)) & proposed_ids)
    return tuple(sorted(found))


def build_proposed_mappings(
    page_level_body_rows: Iterable[eplus_sources.EPlusCandidateRow],
    grate_rows: Iterable[eplus_sources.EPlusCandidateRow],
) -> pd.DataFrame:
    """Build proposal-only E+ base_x_grate mappings from read-only diagnostic evidence."""
    rows: list[dict[str, Any]] = []
    bodies = tuple(page_level_body_rows)
    grates = tuple(grate_rows)
    for body in bodies:
        for grate in grates:
            body_id = _clean(body.proposed_product_id)
            grate_id = _clean(grate.proposed_product_id)
            rows.append(
                {
                    "set_id": f"diagnostic-eplus-{body_id}__{grate_id}",
                    "product_family": PRODUCT_FAMILY,
                    "assembly_model": ASSEMBLY_MODEL,
                    "body_id": body_id,
                    "body_article_number": "",
                    "body_source_url": _clean(body.source_url),
                    "grate_id": grate_id,
                    "grate_article_number": _clean(grate.article_number),
                    "grate_source_url": _clean(grate.source_url),
                    "flow_rate_lps": _clean(body.flow_rate_lps),
                    "water_seal_mm": _clean(body.water_seal_mm),
                    "outlet_dn": _clean(body.outlet_dn),
                    "height_adj_min_mm": _clean(body.height_adj_min_mm),
                    "height_adj_max_mm": _clean(body.height_adj_max_mm),
                    "body_evidence_type": _clean(body.evidence_type),
                    "body_confidence": _clean(body.confidence),
                    "grate_evidence_type": _clean(grate.evidence_type),
                    "grate_confidence": _clean(grate.confidence),
                    "compatibility_evidence_type": COMPATIBILITY_EVIDENCE_TYPE,
                    "compatibility_confidence": COMPATIBILITY_CONFIDENCE,
                    "article_level_compatibility_found": False,
                    "data_quality_status": DATA_QUALITY_STATUS,
                    "missing_evidence": MISSING_EVIDENCE,
                    "safe_to_generate": False,
                    "ready_for_benchmark": False,
                    "ready_for_customer_view": False,
                    "blocking_reason": BLOCKING_REASON,
                    "recommended_next_action": RECOMMENDED_NEXT_ACTION,
                    "production_status_note": PRODUCTION_STATUS_NOTE,
                }
            )
    return pd.DataFrame(rows, columns=PROPOSAL_COLUMNS)


def _count_missing(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return len(df)
    return int((_norm(df, column) == "").sum())


def _bool_true_count(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return 0
    return int(df[column].map(bool).sum())


def _risk_pass_empty(name: str, values: Iterable[str]) -> RiskCheck:
    values = tuple(sorted(set(_clean(value) for value in values if _clean(value))))
    return RiskCheck(name, len(values) == 0, ", ".join(values) if values else "none")


def build_risk_checks(
    proposed_mappings: pd.DataFrame,
    products: pd.DataFrame | None,
    final_assemblies: pd.DataFrame | None,
    final_set_details: pd.DataFrame | None,
    production_behavior_changed: bool = PRODUCTION_BEHAVIOR_CHANGED,
) -> tuple[RiskCheck, ...]:
    proposed_ids = set(_norm(proposed_mappings, "set_id"))
    duplicate_ids = tuple(sorted(set(_norm(proposed_mappings, "set_id")[_norm(proposed_mappings, "set_id").duplicated()])))
    url_set_ids = tuple(sorted(set_id for set_id in proposed_ids if "http" in set_id.lower() or "https" in set_id.lower()))
    missing_hydraulic = {
        column: _count_missing(proposed_mappings, column)
        for column in REQUIRED_HYDRAULIC_FIELDS
    }
    article_level_true = _bool_true_count(proposed_mappings, "article_level_compatibility_found")
    safe_true = _bool_true_count(proposed_mappings, "safe_to_generate")
    benchmark_true = _bool_true_count(proposed_mappings, "ready_for_benchmark")
    customer_true = _bool_true_count(proposed_mappings, "ready_for_customer_view")

    return (
        _risk_pass_empty("duplicate proposed set IDs", duplicate_ids),
        RiskCheck("missing body_id", _count_missing(proposed_mappings, "body_id") == 0, str(_count_missing(proposed_mappings, "body_id"))),
        RiskCheck("missing grate_id", _count_missing(proposed_mappings, "grate_id") == 0, str(_count_missing(proposed_mappings, "grate_id"))),
        _risk_pass_empty("URL-bearing set IDs", url_set_ids),
        _risk_pass_empty("proposed IDs already in Products", _ids_in(products, ("product_id",), proposed_ids)),
        _risk_pass_empty("proposed IDs already in Final_Assemblies", _ids_in(final_assemblies, ("product_id",), proposed_ids)),
        _risk_pass_empty("proposed IDs already in Final_Set_Details", _ids_in(final_set_details, ("set_id", "assembled_product_id"), proposed_ids)),
        RiskCheck("missing hydraulic body fields", all(count == 0 for count in missing_hydraulic.values()), ", ".join(f"{column}={count}" for column, count in missing_hydraulic.items())),
        RiskCheck("missing body source URL", _count_missing(proposed_mappings, "body_source_url") == 0, str(_count_missing(proposed_mappings, "body_source_url"))),
        RiskCheck("missing grate source URL", _count_missing(proposed_mappings, "grate_source_url") == 0, str(_count_missing(proposed_mappings, "grate_source_url"))),
        RiskCheck("article_level_compatibility_found unexpectedly True", article_level_true == 0, str(article_level_true)),
        RiskCheck("safe_to_generate unexpectedly True", safe_true == 0, str(safe_true)),
        RiskCheck("ready_for_benchmark unexpectedly True", benchmark_true == 0, str(benchmark_true)),
        RiskCheck("ready_for_customer_view unexpectedly True", customer_true == 0, str(customer_true)),
        RiskCheck("production behavior changed", not production_behavior_changed, "no" if not production_behavior_changed else "yes"),
    )


def build_report(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    components: pd.DataFrame,
    bom_options: pd.DataFrame,
    final_assemblies: pd.DataFrame,
    final_set_details: pd.DataFrame,
) -> EPlusProposalReport:
    diagnostic = eplus_sources.build_diagnostic(
        products.copy(deep=True),
        comparison.copy(deep=True),
        candidates_all.copy(deep=True),
        components.copy(deep=True),
        bom_options.copy(deep=True),
        final_assemblies.copy(deep=True),
        final_set_details.copy(deep=True),
    )
    proposed_mappings = build_proposed_mappings(diagnostic.page_level_body_evidence, diagnostic.grate_candidates)
    risk_checks = build_risk_checks(proposed_mappings, products, final_assemblies, final_set_details, diagnostic.production_behavior_changed)
    safe_to_generate_count = _bool_true_count(proposed_mappings, "safe_to_generate")
    return EPlusProposalReport(
        input_frame_counts={
            "Products": len(products),
            "Comparison": len(comparison),
            "Candidates_All": len(candidates_all),
            "Components": len(components),
            "BOM_Options": len(bom_options),
            "Final_Assemblies": len(final_assemblies),
            "Final_Set_Details": len(final_set_details),
        },
        diagnostic=diagnostic,
        proposed_mappings=proposed_mappings,
        risk_checks=risk_checks,
        proposed_mapping_count=len(proposed_mappings),
        safe_to_generate_count=safe_to_generate_count,
        blocked_count=len(proposed_mappings) - safe_to_generate_count,
        production_ready=False,
        blocking_reason=BLOCKING_REASON,
        recommended_next_action=RECOMMENDED_NEXT_ACTION,
        production_behavior_changed=diagnostic.production_behavior_changed,
    )


def build_current_report() -> EPlusProposalReport:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(registry_rows)
    products, comparison, components, _evidence, bom_options = pipeline.run_update(candidates_all, default_config())
    final_assemblies = _extract_final_assemblies(products.copy(deep=True))
    final_set_details = _extract_final_set_details(final_assemblies.copy(deep=True), bom_options.copy(deep=True), components.copy(deep=True))
    return build_report(candidates_all, products, comparison, components, bom_options, final_assemblies, final_set_details)


def _pass_text(passed: bool) -> str:
    return "pass" if passed else "block"


def print_report(report: EPlusProposalReport) -> None:
    print("ACO ShowerDrain E+ proposal mapping diagnostic (read-only; no generation)")

    print("\nInput frame counts:")
    for name, count in report.input_frame_counts.items():
        print(f"- {name} = {count}")

    diag = report.diagnostic
    readiness = diag.readiness
    print("\nCurrent E+ diagnostic evidence summary:")
    print(f"- Products E+ rows = {len(diag.output_rows.products)}")
    print(f"- Comparison E+ rows = {len(diag.output_rows.comparison)}")
    print(f"- Candidates_All E+ rows = {len(diag.output_rows.candidates_all)}")
    print(f"- Components/excluded E+ rows = {len(diag.output_rows.components)}")
    print(f"- BOM_Options E+ rows = {len(diag.output_rows.bom_options)}")
    print(f"- Final_Assemblies E+ rows = {len(diag.output_rows.final_assemblies)}")
    print(f"- Final_Set_Details E+ rows = {len(diag.output_rows.final_set_details)}")
    print(f"- E+ page-level body rows = {readiness.eplus_page_level_body_rows}")
    print(f"- E+ article-level body candidates = {readiness.eplus_article_level_body_candidates}")
    print(f"- E+ article-level body incomplete = {readiness.eplus_article_level_body_incomplete}")
    print(f"- E+ grates = {readiness.eplus_grate_candidates}")
    print(f"- hydraulic_complete_candidates = {readiness.hydraulic_complete_candidates}")
    print(f"- likely_assembly_model = {readiness.likely_assembly_model}")
    print(f"- safe_to_generate_eplus_assemblies = {readiness.safe_to_generate_eplus_assemblies}")

    print("\nProposed E+ mapping table:")
    if report.proposed_mappings.empty:
        print("(none)")
    else:
        print(report.proposed_mappings.to_string(index=False))
    print(f"\nproposed E+ mappings = {report.proposed_mapping_count}")

    print("\nRisk checks:")
    for risk in report.risk_checks:
        print(f"- {risk.name}: {_pass_text(risk.passed)} ({risk.detail})")

    print("\nReadiness summary:")
    print(f"- proposed_mappings = {report.proposed_mapping_count}")
    print(f"- safe_to_generate = {report.safe_to_generate_count}")
    print(f"- blocked = {report.blocked_count}")
    print(f"- production_ready = {report.production_ready}")

    print("\nBlocking reason:")
    print(f"- {report.blocking_reason}")

    print("\nRecommended next action:")
    print(f"- {report.recommended_next_action}")

    print("\nproduction behavior changed: no")


def main() -> int:
    print_report(build_current_report())
    return 0


if __name__ == "__main__":
    sys.exit(main())
