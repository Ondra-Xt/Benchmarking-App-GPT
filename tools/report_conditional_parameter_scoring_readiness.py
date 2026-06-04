from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import (
    _extract_conditional_technical_values,
    _extract_final_assemblies,
    _extract_final_set_details,
    _extract_mplus_compound_mappings,
)

STATUS_CONDITIONAL_AVAILABLE = "conditional_values_available"
STATUS_BLOCKED = "blocked_pending_conditional_parameter_scoring"
STATUS_SCENARIO_DATA_AVAILABLE = "scenario_data_available"
STATUS_SCENARIO_NOT_IMPLEMENTED = "scenario_scoring_not_implemented"
STATUS_NO_DEFAULT = "no_default_selected"
STATUS_PRODUCTION_UNCHANGED = "production_behavior_unchanged"
NEXT_ACTION = "implement scoring/export handling for conditional parameter values before production M+ assemblies"

EXPECTED_MPLUS_SET_COUNT = 4
EXPECTED_MPLUS_ROWS_PER_SET = 2
EXPECTED_HEAD_VALUES = {10.0, 20.0}
EXPECTED_FLOW_BY_HEAD = {10.0: 0.40, 20.0: 0.46}
DEFAULT_PATTERNS = ("benchmark_output*.xlsx", "benchmark*.xlsx")

CONDITIONAL_SHEET = "Conditional_Technical_Values"
MPLUS_SHEET = "Mplus_Compound_Mappings"
PRODUCTS_SHEET = "Products"
FINAL_ASSEMBLIES_SHEET = "Final_Assemblies"
FINAL_SET_DETAILS_SHEET = "Final_Set_Details"

REQUIRED_CONDITIONAL_COLUMNS = (
    "set_id",
    "product_family",
    "parameter_name",
    "value",
    "unit",
    "condition_type",
    "condition_value",
    "condition_unit",
    "data_quality_status",
    "safe_to_generate",
    "ready_for_benchmark",
    "ready_for_customer_view",
)
REQUIRED_MPLUS_COLUMNS = (
    "set_id",
    "product_family",
    "flow_rate_lps",
    "selected_default_flow_rate_lps",
    "safe_to_generate",
    "ready_for_benchmark",
    "ready_for_customer_view",
)


class ReadinessReportError(RuntimeError):
    """Raised when conditional parameter readiness input is unsafe or malformed."""


@dataclass(frozen=True)
class ScenarioReadiness:
    scenario: str
    data_available: bool
    scoring_implemented: bool
    policy_approved: bool | None = None
    current_safe_default: bool | None = None
    detail: str = ""


@dataclass(frozen=True)
class MPlusReadiness:
    set_ids: tuple[str, ...]
    rows_per_set: dict[str, int]
    head_10mm_rows: int
    head_20mm_rows: int
    scalar_flow_selected: bool
    selected_default_flow_selected: bool
    safe_to_generate_count: int
    ready_for_benchmark_count: int
    ready_for_customer_view_count: int
    status: str


@dataclass(frozen=True)
class ConditionalScoringReadinessReport:
    data_source: str
    frames: dict[str, pd.DataFrame]
    summary_counts: dict[str, int]
    grouped_counts: dict[str, dict[str, int]]
    mplus_readiness: MPlusReadiness
    scenario_matrix: tuple[ScenarioReadiness, ...]
    blocked_set_ids: tuple[str, ...]
    recommended_actions: tuple[str, ...]
    production_behavior_changed: bool = False


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _present(value: Any) -> bool:
    text = _clean(value).lower()
    return text not in {"", "nan", "none", "null", "unknown", "not_applicable", "n/a", "na"}


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "y", "ja"}


def _numeric(value: Any) -> float | None:
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return float(parsed)


def _series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[column].map(_clean)


def _id_values(df: pd.DataFrame | None, *columns: str) -> set[str]:
    frame = pd.DataFrame() if df is None else df
    values: set[str] = set()
    for column in columns:
        if column in frame.columns:
            values.update(value for value in frame[column].map(_clean) if value)
    return values


def _require_columns(df: pd.DataFrame, required: Iterable[str], frame_name: str) -> None:
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ReadinessReportError(f"{frame_name} is missing required columns: {', '.join(missing)}")


def _group_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in df.columns or df.empty:
        return {}
    counts = Counter(_clean(value) or "(empty)" for value in df[column])
    return dict(sorted(counts.items()))


def pick_xlsx_file(directory: Path, patterns: Iterable[str] = DEFAULT_PATTERNS) -> Path:
    if not directory.exists() or not directory.is_dir():
        raise FileNotFoundError(f"Directory does not exist or is not a directory: {directory}")
    matches: dict[Path, Path] = {}
    for pattern in patterns:
        for candidate in directory.glob(pattern):
            if candidate.is_file():
                matches[candidate.resolve()] = candidate
    if not matches:
        pattern_text = ", ".join(patterns)
        raise FileNotFoundError(f"No benchmark-like .xlsx files found in {directory} for patterns: {pattern_text}")
    return max(matches.values(), key=lambda path: path.stat().st_mtime)


def load_xlsx_frames(path: Path) -> dict[str, pd.DataFrame]:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"XLSX file does not exist: {path}")
    workbook = pd.ExcelFile(path, engine="openpyxl")
    if CONDITIONAL_SHEET not in workbook.sheet_names:
        raise ReadinessReportError(f"{CONDITIONAL_SHEET} sheet is missing from XLSX workbook")
    frames: dict[str, pd.DataFrame] = {}
    for sheet in (CONDITIONAL_SHEET, MPLUS_SHEET, PRODUCTS_SHEET, FINAL_ASSEMBLIES_SHEET, FINAL_SET_DETAILS_SHEET):
        frames[sheet] = pd.read_excel(workbook, sheet_name=sheet, dtype=object) if sheet in workbook.sheet_names else pd.DataFrame()
    return frames


def build_current_frames() -> dict[str, pd.DataFrame]:
    registry_rows, _debug = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    candidates_all = pd.DataFrame(registry_rows)
    products, comparison, components, _evidence, bom_options = pipeline.run_update(candidates_all, default_config())
    final_assemblies = _extract_final_assemblies(products.copy(deep=True))
    final_set_details = _extract_final_set_details(final_assemblies.copy(deep=True), bom_options.copy(deep=True), components.copy(deep=True))
    mplus = _extract_mplus_compound_mappings(
        candidates_all.copy(deep=True),
        products.copy(deep=True),
        comparison.copy(deep=True),
        components.copy(deep=True),
        bom_options.copy(deep=True),
        final_assemblies.copy(deep=True),
        final_set_details.copy(deep=True),
    )
    conditional = _extract_conditional_technical_values(mplus.copy(deep=True))
    return {
        "Candidates_All": candidates_all.copy(deep=True),
        PRODUCTS_SHEET: products.copy(deep=True),
        "Comparison": comparison.copy(deep=True),
        "Components": components.copy(deep=True),
        "BOM_Options": bom_options.copy(deep=True),
        FINAL_ASSEMBLIES_SHEET: final_assemblies.copy(deep=True),
        FINAL_SET_DETAILS_SHEET: final_set_details.copy(deep=True),
        MPLUS_SHEET: mplus.copy(deep=True),
        CONDITIONAL_SHEET: conditional.copy(deep=True),
    }


def _mplus_conditional_rows(conditional: pd.DataFrame) -> pd.DataFrame:
    family = _series(conditional, "product_family").str.lower()
    parameter = _series(conditional, "parameter_name").str.lower()
    condition = _series(conditional, "condition_type").str.lower()
    return conditional[
        family.eq("showerdrain_mplus")
        & parameter.eq("flow_rate_lps")
        & condition.eq("head_water_level")
    ].copy(deep=True)


def _validate_mplus_conditional_rows(mplus_rows: pd.DataFrame, mplus_mappings: pd.DataFrame) -> MPlusReadiness:
    if mplus_rows.empty:
        raise ReadinessReportError("M+ conditional flow rows are missing")

    set_ids = tuple(sorted(value for value in _series(mplus_rows, "set_id").unique() if value))
    if len(set_ids) != EXPECTED_MPLUS_SET_COUNT:
        raise ReadinessReportError(f"M+ conditional rows are malformed: expected 4 set_ids, found {len(set_ids)} ({', '.join(set_ids)})")

    rows_per_set = {set_id: int(count) for set_id, count in _series(mplus_rows, "set_id").value_counts().sort_index().items()}
    bad_counts = {set_id: count for set_id, count in rows_per_set.items() if count != EXPECTED_MPLUS_ROWS_PER_SET}
    if bad_counts:
        raise ReadinessReportError(f"M+ conditional rows are malformed: expected 2 rows per set_id, found {bad_counts}")

    for set_id, rows in mplus_rows.groupby(_series(mplus_rows, "set_id")):
        heads = {_numeric(value) for value in rows["condition_value"]}
        values_by_head = {_numeric(row["condition_value"]): _numeric(row["value"]) for _, row in rows.iterrows()}
        if heads != EXPECTED_HEAD_VALUES:
            raise ReadinessReportError(f"M+ conditional rows are malformed for {set_id}: expected head values 10 and 20 mm, found {sorted(head for head in heads if head is not None)}")
        for head, expected_flow in EXPECTED_FLOW_BY_HEAD.items():
            actual = values_by_head.get(head)
            if actual is None or round(actual, 2) != round(expected_flow, 2):
                raise ReadinessReportError(f"M+ conditional rows are malformed for {set_id}: expected flow_rate_lps {expected_flow} at {int(head)} mm, found {actual}")

    mplus_family = mplus_mappings[_series(mplus_mappings, "product_family").str.lower().eq("showerdrain_mplus")].copy(deep=True)
    if mplus_family.empty:
        raise ReadinessReportError("Mplus_Compound_Mappings has no showerdrain_mplus rows")

    scalar_flow_selected = _series(mplus_family, "flow_rate_lps").map(_present).any()
    selected_default_flow_selected = _series(mplus_family, "selected_default_flow_rate_lps").map(_present).any()
    if scalar_flow_selected:
        raise ReadinessReportError("Invalid M+ default flow selection: scalar flow_rate_lps is filled in Mplus_Compound_Mappings")
    if selected_default_flow_selected:
        raise ReadinessReportError("Invalid M+ default flow selection: selected_default_flow_rate_lps is filled in Mplus_Compound_Mappings")

    safe_count = int(_series(mplus_family, "safe_to_generate").map(_truthy).sum())
    benchmark_count = int(_series(mplus_family, "ready_for_benchmark").map(_truthy).sum())
    customer_count = int(_series(mplus_family, "ready_for_customer_view").map(_truthy).sum())
    if safe_count or benchmark_count or customer_count:
        raise ReadinessReportError(
            "M+ conditional rows are malformed: safe_to_generate/ready flags must remain false "
            f"(safe={safe_count}, benchmark={benchmark_count}, customer={customer_count})"
        )

    return MPlusReadiness(
        set_ids=set_ids,
        rows_per_set=rows_per_set,
        head_10mm_rows=int((_series(mplus_rows, "condition_value").map(_numeric) == 10.0).sum()),
        head_20mm_rows=int((_series(mplus_rows, "condition_value").map(_numeric) == 20.0).sum()),
        scalar_flow_selected=False,
        selected_default_flow_selected=False,
        safe_to_generate_count=safe_count,
        ready_for_benchmark_count=benchmark_count,
        ready_for_customer_view_count=customer_count,
        status=STATUS_BLOCKED,
    )


def _validate_no_production_overlap(conditional: pd.DataFrame, frames: dict[str, pd.DataFrame]) -> None:
    set_ids = _id_values(conditional, "set_id")
    products_overlap = set_ids & _id_values(frames.get(PRODUCTS_SHEET), "product_id", "set_id")
    final_assemblies_overlap = set_ids & _id_values(frames.get(FINAL_ASSEMBLIES_SHEET), "product_id", "assembled_product_id", "set_id")
    final_set_details_overlap = set_ids & _id_values(frames.get(FINAL_SET_DETAILS_SHEET), "product_id", "assembled_product_id", "set_id")
    overlaps = {
        PRODUCTS_SHEET: sorted(products_overlap),
        FINAL_ASSEMBLIES_SHEET: sorted(final_assemblies_overlap),
        FINAL_SET_DETAILS_SHEET: sorted(final_set_details_overlap),
    }
    non_empty = {name: values for name, values in overlaps.items() if values}
    if non_empty:
        raise ReadinessReportError(f"Conditional rows overlap with production output sheets: {non_empty}")


def build_report(frames: dict[str, pd.DataFrame], data_source: str) -> ConditionalScoringReadinessReport:
    conditional = frames.get(CONDITIONAL_SHEET, pd.DataFrame()).copy(deep=True)
    mplus = frames.get(MPLUS_SHEET, pd.DataFrame()).copy(deep=True)
    _require_columns(conditional, REQUIRED_CONDITIONAL_COLUMNS, CONDITIONAL_SHEET)
    _require_columns(mplus, REQUIRED_MPLUS_COLUMNS, MPLUS_SHEET)
    _validate_no_production_overlap(conditional, frames)

    mplus_rows = _mplus_conditional_rows(conditional)
    mplus_readiness = _validate_mplus_conditional_rows(mplus_rows, mplus)
    has_head_10 = mplus_readiness.head_10mm_rows == EXPECTED_MPLUS_SET_COUNT
    has_head_20 = mplus_readiness.head_20mm_rows == EXPECTED_MPLUS_SET_COUNT

    scenario_matrix = (
        ScenarioReadiness("flow_head_10mm", has_head_10, False, detail="M+ flow_rate_lps = 0.4 l/s at head_water_level = 10 mm"),
        ScenarioReadiness("flow_head_20mm", has_head_20, False, detail="M+ flow_rate_lps = 0.46 l/s at head_water_level = 20 mm"),
        ScenarioReadiness("conservative_minimum", has_head_10 and has_head_20, False, policy_approved=False, detail="derivable from conditional observations; not approved for scoring"),
        ScenarioReadiness("maximum_declared", has_head_10 and has_head_20, False, policy_approved=False, detail="derivable from conditional observations; not approved for scoring"),
        ScenarioReadiness("no_scenario_selected", True, False, current_safe_default=True, detail="current safe default; no conditional value is converted into scoring input"),
    )

    grouped = {
        "product_family": _group_counts(conditional, "product_family"),
        "parameter_name": _group_counts(conditional, "parameter_name"),
        "condition_type": _group_counts(conditional, "condition_type"),
        "unit": _group_counts(conditional, "unit"),
        "data_quality_status": _group_counts(conditional, "data_quality_status"),
    }
    blocked_ids = tuple(sorted(set(mplus_readiness.set_ids)))
    return ConditionalScoringReadinessReport(
        data_source=data_source,
        frames={name: frame.copy(deep=True) for name, frame in frames.items()},
        summary_counts={CONDITIONAL_SHEET: len(conditional), MPLUS_SHEET: len(mplus)},
        grouped_counts=grouped,
        mplus_readiness=mplus_readiness,
        scenario_matrix=scenario_matrix,
        blocked_set_ids=blocked_ids,
        recommended_actions=(
            NEXT_ACTION,
            "keep no_scenario_selected as the safe default until conditional-parameter scoring policy is approved",
            "do not select a scalar/default M+ flow value before an explicit policy decision",
        ),
    )


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _print_group(title: str, counts: dict[str, int]) -> None:
    print(f"\n{title}:")
    if not counts:
        print("- none")
    for key, count in counts.items():
        print(f"- {key}: {count}")


def print_report(report: ConditionalScoringReadinessReport) -> None:
    print("Conditional-parameter scoring readiness report (read-only)")
    print("\nSelected workbook or data source:")
    print(f"- {report.data_source}")

    print("\nConditional technical value summary:")
    print(f"- total_rows: {report.summary_counts[CONDITIONAL_SHEET]}")
    for group_name, counts in report.grouped_counts.items():
        for key, count in counts.items():
            print(f"- {group_name}={key}: {count}")
    print(f"- status: {STATUS_CONDITIONAL_AVAILABLE}")

    _print_group("Conditional parameters by product_family", report.grouped_counts["product_family"])
    _print_group("Conditional parameters by parameter_name", report.grouped_counts["parameter_name"])
    _print_group("Conditional parameters by condition_type", report.grouped_counts["condition_type"])
    _print_group("Conditional parameters by unit", report.grouped_counts["unit"])
    _print_group("Conditional parameters by data_quality_status", report.grouped_counts["data_quality_status"])

    mplus = report.mplus_readiness
    print("\nM+ conditional flow readiness:")
    print("- M+ has conditional flow data available.")
    print("- M+ is data-rich but scoring-blocked.")
    print(f"- set_ids: {len(mplus.set_ids)}")
    print(f"- set_id_list: {', '.join(mplus.set_ids)}")
    print(f"- conditional_rows_per_set: {EXPECTED_MPLUS_ROWS_PER_SET}")
    print(f"- 10 mm head rows: {mplus.head_10mm_rows}")
    print(f"- 20 mm head rows: {mplus.head_20mm_rows}")
    print(f"- scalar flow_rate_lps selected: {_yes_no(mplus.scalar_flow_selected)}")
    print(f"- selected_default_flow_rate_lps selected: {_yes_no(mplus.selected_default_flow_selected)}")
    print(f"- safe_to_generate: {mplus.safe_to_generate_count}")
    print(f"- ready_for_benchmark: {mplus.ready_for_benchmark_count}")
    print(f"- ready_for_customer_view: {mplus.ready_for_customer_view_count}")
    print(f"- status: {mplus.status}")
    print(f"- default_selection_status: {STATUS_NO_DEFAULT}")

    print("\nScenario readiness matrix:")
    for row in report.scenario_matrix:
        bits = [f"data_available={row.data_available}"]
        if row.policy_approved is not None:
            bits.append(f"policy_approved={row.policy_approved}")
        bits.append(f"scoring_implemented={row.scoring_implemented}")
        if row.current_safe_default is not None:
            bits.append(f"current_safe_default={row.current_safe_default}")
        bits.append(f"status={STATUS_SCENARIO_DATA_AVAILABLE if row.data_available else 'scenario_data_missing'}")
        bits.append(f"scoring_status={STATUS_SCENARIO_NOT_IMPLEMENTED}")
        print(f"- {row.scenario}: {', '.join(bits)}")
        print(f"  detail: {row.detail}")

    print("\nBlocked sets / products:")
    for set_id in report.blocked_set_ids:
        print(f"- {set_id}: {STATUS_BLOCKED}")

    print("\nRecommended next actions:")
    for action in report.recommended_actions:
        print(f"- {action}")

    print("\nProduction behavior status:")
    print("- Products changed: no")
    print("- BOM changed: no")
    print("- Final_Assemblies changed: no")
    print("- Final_Set_Details changed: no")
    print("- Scoring changed: no")
    print("- Customer-facing output changed: no")
    print("- Production behavior changed: no")
    print(f"- status: {STATUS_PRODUCTION_UNCHANGED}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report conditional-parameter scoring readiness without modifying exports.")
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument("--xlsx", help="Read an existing benchmark XLSX workbook in read-only diagnostic mode.")
    source_group.add_argument("--dir", help="Directory containing benchmark_output*.xlsx / benchmark*.xlsx exports; newest match is selected.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.dir:
            selected = pick_xlsx_file(Path(args.dir))
            print(f"Selected XLSX: {selected}")
            frames = load_xlsx_frames(selected)
            report = build_report(frames, f"xlsx:{selected}")
        elif args.xlsx:
            selected = Path(args.xlsx)
            frames = load_xlsx_frames(selected)
            report = build_report(frames, f"xlsx:{selected}")
        else:
            report = build_report(build_current_frames(), "current pipeline/export data")
        print_report(report)
        return 0
    except (FileNotFoundError, ReadinessReportError) as exc:
        print("Conditional-parameter scoring readiness report (read-only)")
        print(f"ERROR: {exc}")
        print("Production behavior changed: no")
        return 2


if __name__ == "__main__":
    sys.exit(main())
