from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import (
    _extract_final_assemblies,
    _extract_final_set_details,
    _extract_mplus_compound_mappings,
)
from tools import report_mplus_flow_rate_policy as flow_policy

EXPECTED_MPLUS_DRAIN_ARTICLES = ("9010.81.20", "9010.81.21", "9010.81.22", "9010.81.23")
EXPECTED_PROPOSAL_COUNT = 4
READINESS_STATUS_BLOCKED_POLICY = "blocked_policy_not_accepted"
BLOCKING_REASON = flow_policy.BLOCKING_REASON
NEXT_REQUIRED_ACTION = "accept benchmark policy for multi-head-condition flow values before production M+ assemblies"
PRODUCTION_BEHAVIOR_CHANGED = False

# Intentionally empty until a future policy patch explicitly accepts a canonical
# M+ product-flow value for production generation.
ACCEPTED_PRODUCTION_FLOW_POLICIES: frozenset[str] = frozenset()

REQUIRED_PART_ID_COLUMNS = ("channel_body_id", "drain_body_id", "grate_id")
REQUIRED_TECHNICAL_COLUMNS = (
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)


@dataclass(frozen=True)
class GateCondition:
    condition: str
    passed: bool
    actual: str
    required: str


@dataclass(frozen=True)
class RiskCheck:
    risk_check: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class MPlusMappingReadinessRow:
    set_id: str
    drain_body_article_number: str
    channel_body_id: str
    drain_body_id: str
    grate_id: str
    water_seal_mm: str
    outlet_dn: str
    height_adj_min_mm: str
    height_adj_max_mm: str
    flow_rate_lps: str
    flow_rate_lps_10mm_head: str
    flow_rate_lps_20mm_head: str
    selected_default_flow_rate_lps: str
    flow_policy: str
    missing_technical_fields: str
    safe_to_generate: bool
    readiness_status: str
    blocking_reason: str
    required_action: str


@dataclass(frozen=True)
class MPlusProductionReadinessGateReport:
    input_frame_counts: dict[str, int]
    proposal_mapping_count: int
    conditions: tuple[GateCondition, ...]
    per_mapping_rows: tuple[MPlusMappingReadinessRow, ...]
    risk_checks: tuple[RiskCheck, ...]
    production_ready: bool
    readiness_status: str
    blocking_reason: str
    next_required_action: str
    safe_to_generate_count: int
    blocked_count: int
    production_behavior_changed: bool = PRODUCTION_BEHAVIOR_CHANGED


def _clean(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value or "").strip()


def _series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[column].map(_clean)


def _non_empty(df: pd.DataFrame, column: str) -> pd.Series:
    return _series(df, column).ne("")


def _id_set(df: pd.DataFrame | None, *columns: str) -> set[str]:
    frame = pd.DataFrame() if df is None else df
    values: set[str] = set()
    for column in columns:
        if column in frame.columns:
            values.update(value for value in frame[column].map(_clean) if value)
    return values


def _join(values: Iterable[str]) -> str:
    cleaned = [value for value in values if value]
    return ", ".join(cleaned) if cleaned else "none"


def _has_url(value: str) -> bool:
    lowered = value.lower()
    return "http://" in lowered or "https://" in lowered or "http" in lowered


def _policy_accepted(policy: str) -> bool:
    return policy in ACCEPTED_PRODUCTION_FLOW_POLICIES


def _row_required_fields_complete(row: pd.Series) -> bool:
    return all(_clean(row.get(column)) for column in (*REQUIRED_PART_ID_COLUMNS, *REQUIRED_TECHNICAL_COLUMNS))


def _row_has_canonical_flow_or_policy(row: pd.Series) -> bool:
    if _clean(row.get("flow_rate_lps")):
        return True
    return bool(_clean(row.get("selected_default_flow_rate_lps")) and _policy_accepted(_clean(row.get("flow_policy"))))


def _safe_to_generate_consistent(row: pd.Series) -> bool:
    safe = bool(row.get("safe_to_generate"))
    if not safe:
        return True
    return _row_required_fields_complete(row) and _row_has_canonical_flow_or_policy(row) and _policy_accepted(_clean(row.get("flow_policy")))


def _risk_pass_empty(name: str, values: Iterable[str]) -> RiskCheck:
    values_tuple = tuple(values)
    return RiskCheck(name, not values_tuple, _join(values_tuple))


def _proposal_rows(mplus_mappings: pd.DataFrame) -> pd.DataFrame:
    frame = mplus_mappings.copy(deep=True)
    if "drain_body_article_number" not in frame.columns:
        return pd.DataFrame(columns=frame.columns)
    articles = set(EXPECTED_MPLUS_DRAIN_ARTICLES)
    return frame[_series(frame, "drain_body_article_number").isin(articles)].reset_index(drop=True)


def build_readiness_gate_report(
    candidates_all: pd.DataFrame,
    products: pd.DataFrame,
    comparison: pd.DataFrame,
    components: pd.DataFrame,
    bom_options: pd.DataFrame,
    final_assemblies: pd.DataFrame,
    final_set_details: pd.DataFrame,
    mplus_mappings: pd.DataFrame,
) -> MPlusProductionReadinessGateReport:
    """Build a read-only production-readiness gate from diagnostic M+ rows."""
    proposal_df = _proposal_rows(mplus_mappings)
    set_ids = tuple(_series(proposal_df, "set_id"))
    set_id_set = {value for value in set_ids if value}
    products_overlap = tuple(sorted(set_id_set & _id_set(products, "product_id")))
    final_assemblies_overlap = tuple(sorted(set_id_set & _id_set(final_assemblies, "product_id")))
    final_set_details_overlap = tuple(sorted(set_id_set & _id_set(final_set_details, "set_id", "assembled_product_id")))
    url_set_ids = tuple(sorted(value for value in set_id_set if _has_url(value)))

    missing_counts = {
        column: int((~_non_empty(proposal_df, column)).sum())
        for column in (*REQUIRED_PART_ID_COLUMNS, *REQUIRED_TECHNICAL_COLUMNS)
    }
    flow_rate_filled = _non_empty(proposal_df, "flow_rate_lps")
    selected_default_filled = _non_empty(proposal_df, "selected_default_flow_rate_lps")
    policy_accepted = _series(proposal_df, "flow_policy").map(_policy_accepted)
    canonical_flow_or_policy = flow_rate_filled | (selected_default_filled & policy_accepted)
    safe_to_generate = proposal_df["safe_to_generate"].map(bool) if "safe_to_generate" in proposal_df.columns else pd.Series([False] * len(proposal_df))
    ready_for_benchmark = proposal_df["ready_for_benchmark"].map(bool) if "ready_for_benchmark" in proposal_df.columns else pd.Series([False] * len(proposal_df))
    ready_for_customer_view = proposal_df["ready_for_customer_view"].map(bool) if "ready_for_customer_view" in proposal_df.columns else pd.Series([False] * len(proposal_df))

    conditions = (
        GateCondition("exactly 4 proposal mappings exist", len(proposal_df) == EXPECTED_PROPOSAL_COUNT, str(len(proposal_df)), str(EXPECTED_PROPOSAL_COUNT)),
        GateCondition("no proposed set_id overlaps Products.product_id", not products_overlap, _join(products_overlap), "none"),
        GateCondition("no proposed set_id overlaps Final_Assemblies.product_id", not final_assemblies_overlap, _join(final_assemblies_overlap), "none"),
        GateCondition("no proposed set_id overlaps Final_Set_Details.set_id or assembled_product_id", not final_set_details_overlap, _join(final_set_details_overlap), "none"),
        GateCondition("no proposed set_id contains http or https", not url_set_ids, _join(url_set_ids), "none"),
        GateCondition("all rows have channel_body_id", missing_counts["channel_body_id"] == 0, str(missing_counts["channel_body_id"]), "0 missing"),
        GateCondition("all rows have drain_body_id", missing_counts["drain_body_id"] == 0, str(missing_counts["drain_body_id"]), "0 missing"),
        GateCondition("all rows have grate_id", missing_counts["grate_id"] == 0, str(missing_counts["grate_id"]), "0 missing"),
        GateCondition("all rows have water_seal_mm", missing_counts["water_seal_mm"] == 0, str(missing_counts["water_seal_mm"]), "0 missing"),
        GateCondition("all rows have outlet_dn", missing_counts["outlet_dn"] == 0, str(missing_counts["outlet_dn"]), "0 missing"),
        GateCondition("all rows have height_adj_min_mm", missing_counts["height_adj_min_mm"] == 0, str(missing_counts["height_adj_min_mm"]), "0 missing"),
        GateCondition("all rows have height_adj_max_mm", missing_counts["height_adj_max_mm"] == 0, str(missing_counts["height_adj_max_mm"]), "0 missing"),
        GateCondition("all rows have flow_rate_lps or an accepted policy that selects a canonical flow value", bool(canonical_flow_or_policy.all()) if len(proposal_df) else False, f"{int(canonical_flow_or_policy.sum())}/{len(proposal_df)} ready", "all rows"),
        GateCondition("flow_policy is accepted for production", bool(policy_accepted.all()) if len(proposal_df) else False, _join(sorted(set(_series(proposal_df, "flow_policy")))) or "none", f"one of {sorted(ACCEPTED_PRODUCTION_FLOW_POLICIES)}"),
        GateCondition("selected_default_flow_rate_lps is non-empty only after policy acceptance", not bool((selected_default_filled & ~policy_accepted).any()), f"violations={int((selected_default_filled & ~policy_accepted).sum())}", "0 violations"),
        GateCondition("safe_to_generate is True only after all required fields and policy are complete", all(_safe_to_generate_consistent(row) for _, row in proposal_df.iterrows()), f"violations={sum(not _safe_to_generate_consistent(row) for _, row in proposal_df.iterrows())}", "0 violations"),
    )

    per_mapping_rows: list[MPlusMappingReadinessRow] = []
    for _, row in proposal_df.iterrows():
        per_mapping_rows.append(
            MPlusMappingReadinessRow(
                set_id=_clean(row.get("set_id")),
                drain_body_article_number=_clean(row.get("drain_body_article_number")),
                channel_body_id=_clean(row.get("channel_body_id")),
                drain_body_id=_clean(row.get("drain_body_id")),
                grate_id=_clean(row.get("grate_id")),
                water_seal_mm=_clean(row.get("water_seal_mm")),
                outlet_dn=_clean(row.get("outlet_dn")),
                height_adj_min_mm=_clean(row.get("height_adj_min_mm")),
                height_adj_max_mm=_clean(row.get("height_adj_max_mm")),
                flow_rate_lps=_clean(row.get("flow_rate_lps")),
                flow_rate_lps_10mm_head=_clean(row.get("flow_rate_lps_10mm_head")),
                flow_rate_lps_20mm_head=_clean(row.get("flow_rate_lps_20mm_head")),
                selected_default_flow_rate_lps=_clean(row.get("selected_default_flow_rate_lps")),
                flow_policy=_clean(row.get("flow_policy")),
                missing_technical_fields=_clean(row.get("missing_technical_fields")),
                safe_to_generate=bool(row.get("safe_to_generate")),
                readiness_status=READINESS_STATUS_BLOCKED_POLICY,
                blocking_reason=_clean(row.get("blocking_reason")) or BLOCKING_REASON,
                required_action=NEXT_REQUIRED_ACTION,
            )
        )

    risk_checks = (
        _risk_pass_empty("M+ diagnostic set IDs in Products.product_id", products_overlap),
        _risk_pass_empty("M+ diagnostic set IDs in Final_Assemblies.product_id", final_assemblies_overlap),
        _risk_pass_empty("M+ diagnostic set IDs in Final_Set_Details", final_set_details_overlap),
        _risk_pass_empty("URL-bearing set IDs", url_set_ids),
        RiskCheck("missing channel body ID", missing_counts["channel_body_id"] == 0, str(missing_counts["channel_body_id"])),
        RiskCheck("missing drain body ID", missing_counts["drain_body_id"] == 0, str(missing_counts["drain_body_id"])),
        RiskCheck("missing grate ID", missing_counts["grate_id"] == 0, str(missing_counts["grate_id"])),
        RiskCheck("missing WS/DN/height fields", all(missing_counts[column] == 0 for column in REQUIRED_TECHNICAL_COLUMNS), ", ".join(f"{column}={missing_counts[column]}" for column in REQUIRED_TECHNICAL_COLUMNS)),
        RiskCheck("flow_rate_lps filled without accepted policy", not bool((flow_rate_filled & ~policy_accepted).any()), str(int((flow_rate_filled & ~policy_accepted).sum()))),
        RiskCheck("selected_default_flow_rate_lps filled without accepted policy", not bool((selected_default_filled & ~policy_accepted).any()), str(int((selected_default_filled & ~policy_accepted).sum()))),
        RiskCheck("safe_to_generate True while policy is unaccepted", not bool((safe_to_generate & ~policy_accepted).any()), str(int((safe_to_generate & ~policy_accepted).sum()))),
        RiskCheck("ready_for_benchmark True while policy is unaccepted", not bool((ready_for_benchmark & ~policy_accepted).any()), str(int((ready_for_benchmark & ~policy_accepted).sum()))),
        RiskCheck("ready_for_customer_view True while policy is unaccepted", not bool((ready_for_customer_view & ~policy_accepted).any()), str(int((ready_for_customer_view & ~policy_accepted).sum()))),
        RiskCheck("production behavior changed", not PRODUCTION_BEHAVIOR_CHANGED, "no"),
    )

    production_ready = all(condition.passed for condition in conditions)
    return MPlusProductionReadinessGateReport(
        input_frame_counts={
            "Products": len(products),
            "Comparison": len(comparison),
            "Candidates_All": len(candidates_all),
            "Components": len(components),
            "BOM_Options": len(bom_options),
            "Final_Assemblies": len(final_assemblies),
            "Final_Set_Details": len(final_set_details),
            "Mplus_Compound_Mappings": len(proposal_df),
        },
        proposal_mapping_count=len(proposal_df),
        conditions=conditions,
        per_mapping_rows=tuple(per_mapping_rows),
        risk_checks=risk_checks,
        production_ready=production_ready,
        readiness_status="production_ready" if production_ready else READINESS_STATUS_BLOCKED_POLICY,
        blocking_reason="" if production_ready else BLOCKING_REASON,
        next_required_action="" if production_ready else NEXT_REQUIRED_ACTION,
        safe_to_generate_count=int(safe_to_generate.sum()),
        blocked_count=int((~safe_to_generate).sum()) if len(proposal_df) else 0,
    )


def build_current_report() -> MPlusProductionReadinessGateReport:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(registry_rows)
    products, comparison, components, _evidence, bom_options = pipeline.run_update(candidates_all, default_config())
    final_assemblies = _extract_final_assemblies(products.copy(deep=True))
    final_set_details = _extract_final_set_details(final_assemblies.copy(deep=True), bom_options.copy(deep=True), components.copy(deep=True))
    mplus_mappings = _extract_mplus_compound_mappings(
        candidates_all.copy(deep=True),
        products.copy(deep=True),
        comparison.copy(deep=True),
        components.copy(deep=True),
        bom_options.copy(deep=True),
        final_assemblies.copy(deep=True),
        final_set_details.copy(deep=True),
    )
    return build_readiness_gate_report(
        candidates_all.copy(deep=True),
        products.copy(deep=True),
        comparison.copy(deep=True),
        components.copy(deep=True),
        bom_options.copy(deep=True),
        final_assemblies.copy(deep=True),
        final_set_details.copy(deep=True),
        mplus_mappings.copy(deep=True),
    )


def _pass_text(value: bool) -> str:
    return "pass" if value else "block"


def _display(value: str) -> str:
    return value if value else "(empty)"


def print_report(report: MPlusProductionReadinessGateReport) -> None:
    print("ACO ShowerDrain M+ production-readiness gate diagnostic (read-only; no generation)")
    print("production behavior changed: no")

    print("\nInput frame counts:")
    for name, count in report.input_frame_counts.items():
        print(f"- {name} = {count}")

    print(f"\nM+ proposal mapping count = {report.proposal_mapping_count}")

    print("\nReadiness gate condition table:")
    print("| condition | status | actual | required |")
    print("| --- | --- | --- | --- |")
    for condition in report.conditions:
        print(f"| {condition.condition} | {_pass_text(condition.passed)} | {condition.actual} | {condition.required} |")

    print("\nPer-mapping readiness rows:")
    print(
        "| set_id | drain_body_article_number | channel_body_id | drain_body_id | grate_id | "
        "water_seal_mm | outlet_dn | height_adj_min_mm | height_adj_max_mm | flow_rate_lps | "
        "flow_rate_lps_10mm_head | flow_rate_lps_20mm_head | selected_default_flow_rate_lps | "
        "flow_policy | missing_technical_fields | safe_to_generate | readiness_status | blocking_reason | required_action |"
    )
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in report.per_mapping_rows:
        print(
            f"| {row.set_id} | {row.drain_body_article_number} | {row.channel_body_id} | {row.drain_body_id} | "
            f"{row.grate_id} | {row.water_seal_mm} | {row.outlet_dn} | {row.height_adj_min_mm} | "
            f"{row.height_adj_max_mm} | {_display(row.flow_rate_lps)} | {_display(row.flow_rate_lps_10mm_head)} | "
            f"{_display(row.flow_rate_lps_20mm_head)} | {_display(row.selected_default_flow_rate_lps)} | {row.flow_policy} | "
            f"{row.missing_technical_fields} | {row.safe_to_generate} | {row.readiness_status} | "
            f"{row.blocking_reason} | {row.required_action} |"
        )

    print("\nProduction-ready summary:")
    print(f"- production_ready = {report.production_ready}")
    print(f"- readiness_status = {report.readiness_status}")
    print(f"- safe_to_generate = {report.safe_to_generate_count}")
    print(f"- blocked = {report.blocked_count}")

    print("\nBlocking reason:")
    print(f"- {report.blocking_reason or 'none'}")

    print("\nNext required action:")
    print(f"- {report.next_required_action or 'none'}")

    print("\nRisk checks:")
    for risk in report.risk_checks:
        print(f"- {risk.risk_check}: {_pass_text(risk.passed)} ({risk.detail})")

    print("\nproduction behavior changed: no")


def main() -> int:
    print_report(build_current_report())
    return 0


if __name__ == "__main__":
    sys.exit(main())
