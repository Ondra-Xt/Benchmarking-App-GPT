"""Read-only policy review for conditional ACO ShowerDrain M+ flow values.

This report validates the canonical blocked/default state and the two explicit
benchmark scenarios. It never writes to the workbook, selects a scalar default,
or enables customer presentation.
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

EXPECTED_ARTICLES = frozenset({"9010.81.20", "9010.81.21", "9010.81.22", "9010.81.23"})
EXPECTED_SHEET_COUNTS = {
    "Products": 88,
    "Comparison": 88,
    "Scoring_Field_Coverage": 88,
    "BOM_Options": 251,
    "Final_Assemblies": 62,
    "Final_Set_Details": 62,
    "Conditional_Technical_Values": 24,
    "Mplus_Compound_Mappings": 4,
    "Scoring_Scenarios": 3,
    "Comparison_flow_head_10mm": 88,
    "Comparison_flow_head_20mm": 88,
}
REQUIRED_SHEETS = (
    "Products",
    "Comparison",
    "Final_Assemblies",
    "Final_Set_Details",
    "Mplus_Compound_Mappings",
    "Conditional_Technical_Values",
    "Scoring_Scenarios",
    "Comparison_flow_head_10mm",
    "Comparison_flow_head_20mm",
)

DEFAULT_BLOCKED = "default_blocked_requires_condition"
SCENARIO_10_READY = "scenario_10mm_benchmark_ready"
SCENARIO_20_READY = "scenario_20mm_benchmark_ready"
CUSTOMER_APPROVAL_REQUIRED = "customer_conditional_presentation_requires_approval"
INVALID = "blocked_invalid_or_missing_condition"
POLICY_GATE = "requires_manual_customer_policy_approval"
BLOCKED_REASON_FRAGMENT = "conditional_parameter_scoring"
RECOMMENDED_NEXT_ACTION = (
    "approve an explicit-condition customer presentation model; do not select a default scalar flow."
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _empty(value: Any) -> bool:
    return _text(value).lower() in {"", "nan", "none", "null"}


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "1.0", "true", "yes", "y"}


def _falsey(value: Any) -> bool:
    if isinstance(value, bool):
        return not value
    return _text(value).lower() in {"0", "0.0", "false", "no", "n"}


def _numeric_equal(value: Any, expected: float) -> bool:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return bool(pd.notna(parsed) and abs(float(parsed) - expected) < 1e-9)


def _series(frame: pd.DataFrame, column: str, default: Any = "") -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)


def _mplus_rows(frame: pd.DataFrame) -> pd.DataFrame:
    families = pd.Series(False, index=frame.index)
    for column in ("product_family", "family", "assembled_family"):
        families |= _series(frame, column).map(lambda value: _text(value).lower()).eq("showerdrain_mplus")
    identities = pd.Series(False, index=frame.index)
    for column in ("product_id", "assembled_product_id", "set_id"):
        identities |= _series(frame, column).map(lambda value: _text(value).lower()).str.startswith(
            "aco-assembled-showerdrain-mplus-"
        )
    return frame[families | identities].copy(deep=True)


def _id(row: pd.Series) -> str:
    for column in ("product_id", "assembled_product_id", "set_id"):
        value = _text(row.get(column))
        if value:
            return value
    return ""


def _row_by_id(frame: pd.DataFrame, assembly_id: str) -> pd.Series | None:
    for column in ("product_id", "assembled_product_id", "set_id"):
        if column not in frame.columns:
            continue
        matches = frame[_series(frame, column).map(_text).eq(assembly_id)]
        if len(matches) == 1:
            return matches.iloc[0]
    return None


def _all_rows(rows: list[pd.Series | None], predicate: Any, column: str) -> bool:
    return all(row is not None and predicate(row.get(column)) for row in rows)


def _scenario_registry_valid(frame: pd.DataFrame) -> bool:
    expected = {
        "no_scenario_selected": {
            "condition_type": "", "condition_value": None, "condition_unit": "",
            "is_default": True, "scoring_enabled": False, "customer_view_enabled": False,
        },
        "flow_head_10mm": {
            "condition_type": "head_water_level", "condition_value": 10.0, "condition_unit": "mm",
            "is_default": False, "scoring_enabled": True, "customer_view_enabled": False,
        },
        "flow_head_20mm": {
            "condition_type": "head_water_level", "condition_value": 20.0, "condition_unit": "mm",
            "is_default": False, "scoring_enabled": True, "customer_view_enabled": False,
        },
    }
    if len(frame) != len(expected) or "scenario_id" not in frame.columns:
        return False
    for scenario_id, policy in expected.items():
        rows = frame[_series(frame, "scenario_id").map(_text).eq(scenario_id)]
        if len(rows) != 1:
            return False
        row = rows.iloc[0]
        condition_value = policy["condition_value"]
        if (
            _text(row.get("condition_type")) != policy["condition_type"]
            or _text(row.get("condition_unit")) != policy["condition_unit"]
            or _truthy(row.get("is_default")) != policy["is_default"]
            or _truthy(row.get("scoring_enabled")) != policy["scoring_enabled"]
            or _truthy(row.get("customer_view_enabled")) != policy["customer_view_enabled"]
        ):
            return False
        if condition_value is None:
            if not _empty(row.get("condition_value")):
                return False
        elif not _numeric_equal(row.get("condition_value"), condition_value):
            return False
    return True


def _scenario_check(row: pd.Series | None, *, head: int, flow: float) -> bool:
    return bool(
        row is not None
        and _numeric_equal(row.get("flow_rate_lps"), flow)
        and _text(row.get("flow_rate_resolution_source")) == "Conditional_Technical_Values"
        and _text(row.get("flow_rate_resolution_status")) == "resolved_from_condition"
        and _text(row.get("flow_rate_condition_type")) == "head_water_level"
        and _numeric_equal(row.get("flow_rate_condition_value"), float(head))
        and _text(row.get("flow_rate_condition_unit")) == "mm"
        and not _empty(row.get("flow_rate_condition_label"))
        and _truthy(row.get("scenario_ready_for_benchmark"))
    )


def build_mplus_conditional_customer_policy_report(
    sheets: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    """Return one policy-review row per M+ assembly without mutating inputs."""
    missing = [name for name in REQUIRED_SHEETS if name not in sheets]
    if missing:
        raise ValueError(f"Missing required workbook sheets: {', '.join(missing)}")

    scoped = {name: _mplus_rows(pd.DataFrame(sheets[name])) for name in REQUIRED_SHEETS}
    mappings = scoped["Mplus_Compound_Mappings"]
    reviews: list[dict[str, Any]] = []

    for _, mapping in mappings.sort_values("set_id", kind="stable").iterrows():
        assembly_id = _id(mapping)
        product = _row_by_id(scoped["Products"], assembly_id)
        comparison = _row_by_id(scoped["Comparison"], assembly_id)
        final = _row_by_id(scoped["Final_Assemblies"], assembly_id)
        detail = _row_by_id(scoped["Final_Set_Details"], assembly_id)
        scenario_10 = _row_by_id(scoped["Comparison_flow_head_10mm"], assembly_id)
        scenario_20 = _row_by_id(scoped["Comparison_flow_head_20mm"], assembly_id)
        canonical_rows = [product, comparison, final, detail]

        conditions = scoped["Conditional_Technical_Values"]
        conditions = conditions[
            _series(conditions, "set_id").map(_text).eq(assembly_id)
            & _series(conditions, "parameter_name").map(_text).eq("flow_rate_lps")
        ]
        head_values = pd.to_numeric(_series(conditions, "condition_value"), errors="coerce")
        condition_10 = conditions[head_values.eq(10.0)]
        condition_20 = conditions[head_values.eq(20.0)]

        checks = {
            "present_in_all_canonical_sheets": all(row is not None for row in canonical_rows),
            "assembly_model": _text(mapping.get("assembly_model")) == "channel_body_x_drain_body_x_grate",
            "known_channel_body_article": _text(mapping.get("drain_body_article_number")) in EXPECTED_ARTICLES,
            "scalar_flow_empty": _empty(mapping.get("flow_rate_lps"))
            and _all_rows(canonical_rows, _empty, "flow_rate_lps"),
            "selected_default_empty": _all_rows(
                [mapping, *canonical_rows], _empty, "selected_default_flow_rate_lps"
            ),
            "exactly_two_conditional_values": len(conditions) == 2,
            "condition_10mm_is_0_40": len(condition_10) == 1
            and _numeric_equal(condition_10.iloc[0].get("value"), 0.40),
            "condition_20mm_is_0_46": len(condition_20) == 1
            and _numeric_equal(condition_20.iloc[0].get("value"), 0.46),
            "condition_metadata": len(conditions) == 2
            and _series(conditions, "condition_type").map(_text).eq("head_water_level").all()
            and _series(conditions, "condition_unit").map(_text).eq("mm").all()
            and _series(conditions, "unit").map(_text).eq("l/s").all(),
            "default_benchmark_blocked": _all_rows(canonical_rows, _falsey, "ready_for_benchmark"),
            "default_customer_blocked": _all_rows(canonical_rows, _falsey, "ready_for_customer_view"),
            "customer_view_disabled": all(
                row is not None and not _truthy(row.get("customer_view_enabled", False))
                for row in canonical_rows
            ),
            "blocked_reason_requires_condition": all(
                row is not None
                and (
                    BLOCKED_REASON_FRAGMENT in _text(row.get("blocked_reason"))
                    or BLOCKED_REASON_FRAGMENT in _text(row.get("blocking_reason"))
                )
                for row in canonical_rows
            ),
            "scenario_registry_requires_explicit_selection": _scenario_registry_valid(
                pd.DataFrame(sheets["Scoring_Scenarios"])
            ),
            "scenario_10mm_ready": _scenario_check(scenario_10, head=10, flow=0.40),
            "scenario_20mm_ready": _scenario_check(scenario_20, head=20, flow=0.46),
        }
        valid = all(checks.values())
        states = (
            [DEFAULT_BLOCKED, SCENARIO_10_READY, SCENARIO_20_READY, CUSTOMER_APPROVAL_REQUIRED]
            if valid
            else [INVALID]
        )
        failures = [name for name, passed in checks.items() if not passed]
        reviews.append({
            "assembly_id": assembly_id,
            "channel_body_article_number": _text(mapping.get("drain_body_article_number")),
            **{f"check_{name}": passed for name, passed in checks.items()},
            "policy_states": ",".join(states),
            "classification": CUSTOMER_APPROVAL_REQUIRED if valid else INVALID,
            "policy_gate": POLICY_GATE if valid else INVALID,
            "blocking_reasons": ",".join(failures),
            "recommended_next_action": (
                RECOMMENDED_NEXT_ACTION if valid else "repair invalid or missing conditional evidence before policy approval."
            ),
        })

    return pd.DataFrame(reviews)


def workbook_diagnostics(
    sheets: Mapping[str, pd.DataFrame], review: pd.DataFrame
) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {
        name: len(sheets[name]) for name in EXPECTED_SHEET_COUNTS if name in sheets
    }
    mplus_conditions = _mplus_rows(pd.DataFrame(sheets["Conditional_Technical_Values"]))
    diagnostics.update({
        "Mplus_assemblies_reviewed": len(review),
        "Mplus_conditional_rows": len(mplus_conditions),
        "default_blocked_rows": int(review.get("check_default_benchmark_blocked", pd.Series(dtype=bool)).sum()),
        "scenario_10mm_ready_rows": int(review.get("check_scenario_10mm_ready", pd.Series(dtype=bool)).sum()),
        "scenario_20mm_ready_rows": int(review.get("check_scenario_20mm_ready", pd.Series(dtype=bool)).sum()),
        "customer_view_enabled_mplus_rows": int(
            (~review.get("check_customer_view_disabled", pd.Series(dtype=bool))).sum()
        ),
        "invalid_conditional_rows": int(review.get("classification", pd.Series(dtype=str)).eq(INVALID).sum()),
        "selected_scalar_defaults": int(
            (~review.get("check_selected_default_empty", pd.Series(dtype=bool))).sum()
        ),
    })
    return diagnostics


def policy_gate(review: pd.DataFrame) -> str:
    expected_ids = len(review) == 4 and review["assembly_id"].nunique() == 4
    return (
        POLICY_GATE
        if expected_ids and review["classification"].eq(CUSTOMER_APPROVAL_REQUIRED).all()
        else INVALID
    )


def load_workbook_sheets(path: str | Path) -> dict[str, pd.DataFrame]:
    workbook_path = Path(path)
    if not workbook_path.is_file():
        raise FileNotFoundError(f"XLSX file does not exist: {workbook_path}")
    with pd.ExcelFile(workbook_path, engine="openpyxl") as workbook:
        missing = [name for name in REQUIRED_SHEETS if name not in workbook.sheet_names]
        if missing:
            raise ValueError(f"Missing required workbook sheets: {', '.join(missing)}")
        return {name: pd.read_excel(workbook, sheet_name=name) for name in workbook.sheet_names}


def print_report(review: pd.DataFrame, diagnostics: Mapping[str, Any], xlsx: Path) -> None:
    print("ACO ShowerDrain M+ conditional customer policy review")
    print("Mode: read-only policy evaluation")
    print(f"Workbook: {xlsx}")
    print("Policy: M+ is scoreable only after explicit condition selection.")
    print("Customer presentation: show the selected flow and its condition together.")
    print("Default policy: no unconditional scalar default is allowed; canonical default rows remain blocked.")
    print()
    for _, row in review.iterrows():
        print(
            f"- {row['assembly_id']} ({row['channel_body_article_number']}): "
            f"{row['policy_states']}"
        )
        if row["blocking_reasons"]:
            print(f"  failures: {row['blocking_reasons']}")
    print()
    labels = (
        ("M+ assemblies reviewed", "Mplus_assemblies_reviewed"),
        ("default blocked rows", "default_blocked_rows"),
        ("10 mm scenario-ready rows", "scenario_10mm_ready_rows"),
        ("20 mm scenario-ready rows", "scenario_20mm_ready_rows"),
        ("customer-view-enabled M+ rows", "customer_view_enabled_mplus_rows"),
        ("invalid conditional rows", "invalid_conditional_rows"),
        ("selected scalar defaults", "selected_scalar_defaults"),
    )
    for label, key in labels:
        print(f"{label}: {diagnostics[key]}")
    print()
    print(f"OVERALL: {policy_gate(review)}")
    print(f"Recommended next action: {RECOMMENDED_NEXT_ACTION}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xlsx", required=True, type=Path, help="Canonical benchmark XLSX to inspect")
    args = parser.parse_args(argv)
    before = hashlib.sha256(args.xlsx.read_bytes()).hexdigest() if args.xlsx.is_file() else ""
    try:
        sheets = load_workbook_sheets(args.xlsx)
        review = build_mplus_conditional_customer_policy_report(sheets)
        diagnostics = workbook_diagnostics(sheets, review)
        print_report(review, diagnostics, args.xlsx)
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    after = hashlib.sha256(args.xlsx.read_bytes()).hexdigest()
    if before != after:
        print("ERROR: report execution mutated the input XLSX", file=sys.stderr)
        return 1
    return 0 if policy_gate(review) == POLICY_GATE else 1


if __name__ == "__main__":
    raise SystemExit(main())
