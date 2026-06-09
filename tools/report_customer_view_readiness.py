"""Read-only customer-view readiness policy report for the canonical ACO benchmark.

The report deliberately consumes canonical export sheets without changing pipeline frames,
customer-view flags, or workbook content.  With no ``--xlsx`` argument it builds a
validator-approved workbook in a temporary directory and deletes it after reporting.
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app_export import AppExportValidationError
from src.canonical_aco_export import export_canonical_aco_workbook
from src.config import default_config

CATEGORIES = (
    "customer_view_ready",
    "benchmark_ready_but_customer_disabled",
    "blocked_partial",
    "blocked_conditional",
    "diagnostic_only_not_product",
    "not_applicable",
)

ASSEMBLY_SHEETS = ("Final_Assemblies", "Final_Set_Details")
INPUT_SHEETS = (
    "Products",
    *ASSEMBLY_SHEETS,
    "Conditional_Technical_Values",
    "Eplus_Compatible_Grate_Evidence",
    "Bline_Source_Evidence",
)

READY_REASON = (
    "Complete, non-conditional production assembly; benchmark and existing customer-view "
    "policy flags are enabled."
)
READY_ACTION = "No readiness-policy action; retain normal publication controls."
CPLUS_REASON = (
    "C+ is production-ready for benchmark use, but customer publication remains disabled "
    "pending a separate approval/review."
)
CPLUS_ACTION = "Complete separate customer-facing publication review; do not auto-enable C+."
MPLUS_REASON = (
    "M+ has no supported default scalar flow_rate_lps; conditional 10 mm and 20 mm head "
    "values are preserved separately."
)
MPLUS_ACTION = (
    "Define and approve customer/scoring handling for conditional flow values; do not select "
    "an unsupported default."
)
EASYFLOW_REASON = (
    "Easyflow flow_rate_lps, height_adj_min_mm, and height_adj_max_mm are ambiguous at "
    "article/variant granularity."
)
EASYFLOW_ACTION = (
    "Resolve article-level variant attribution for flow and height before customer publication."
)
EPLUS_REASON = (
    "E+ is proposal/diagnostic evidence only because explicit article-level body-to-grate "
    "compatibility is missing."
)
EPLUS_ACTION = (
    "Collect explicit article-level E+ body-to-grate compatibility before generating products."
)
BLINE_REASON = (
    "B-line evidence describes integral all-in-one finished-set articles, not generated "
    "base_x_grate production assemblies."
)
BLINE_ACTION = (
    "Model verified finished-set articles directly if approved; do not generate base_x_grate rows."
)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _column(frame: pd.DataFrame, name: str, default: Any = "") -> pd.Series:
    if name in frame.columns:
        return frame[name]
    return pd.Series([default] * len(frame), index=frame.index)


def _assembly_id(row: pd.Series, source_sheet: str) -> str:
    candidates = (
        ("product_id", "assembled_product_id", "set_id")
        if source_sheet == "Final_Assemblies"
        else ("assembled_product_id", "set_id", "product_id")
    )
    for column in candidates:
        value = _text(row.get(column))
        if value:
            return value
    return ""


def _family(row: pd.Series) -> str:
    family = _text(row.get("assembled_family") or row.get("product_family")).lower()
    if family:
        return family
    product_id = _text(row.get("product_id") or row.get("assembled_product_id")).lower()
    for token, value in (
        ("showerdrain-cplus", "showerdrain_cplus"),
        ("showerdrain-mplus", "showerdrain_mplus"),
        ("showerdrain-splus", "showerdrain_splus"),
        ("showerdrain-c-", "showerdrain_c"),
        ("easyflowplus", "easyflowplus"),
        ("easyflow", "easyflow"),
    ):
        if token in product_id:
            return value
    return "unknown"


def _complete(row: pd.Series) -> bool:
    if "is_complete_technical_data" in row.index:
        return _truthy(row.get("is_complete_technical_data"))
    missing = _text(row.get("missing_technical_fields"))
    status = _text(row.get("data_quality_status")).lower()
    return not missing and status not in {"partial", "missing"}


def _product_policy_lookup(products: pd.DataFrame) -> dict[str, dict[str, bool]]:
    lookup: dict[str, dict[str, bool]] = {}
    for _, row in products.iterrows():
        product_id = _text(row.get("product_id"))
        if not product_id:
            continue
        lookup[product_id] = {
            "ready_for_benchmark": _truthy(row.get("ready_for_benchmark")),
            "ready_for_customer_view": _truthy(row.get("ready_for_customer_view")),
            "customer_view_enabled": _truthy(row.get("customer_view_enabled")),
        }
    return lookup


def _classify_assembly(
    row: pd.Series,
    *,
    source_sheet: str,
    product_policy: Mapping[str, Mapping[str, bool]],
    conditional_set_ids: set[str],
) -> dict[str, Any]:
    row_id = _assembly_id(row, source_sheet)
    family = _family(row)
    policy = product_policy.get(row_id, {})
    ready_for_benchmark = _truthy(row.get("ready_for_benchmark")) or bool(
        policy.get("ready_for_benchmark", False)
    )
    ready_for_customer_view = _truthy(row.get("ready_for_customer_view")) or bool(
        policy.get("ready_for_customer_view", False)
    )
    customer_view_enabled = bool(policy.get("customer_view_enabled", False))
    complete = _complete(row)
    set_id = _text(row.get("set_id"))

    if family == "showerdrain_cplus":
        category, reason, action = (
            "benchmark_ready_but_customer_disabled",
            CPLUS_REASON,
            CPLUS_ACTION,
        )
    elif family == "showerdrain_mplus" or row_id in conditional_set_ids or set_id in conditional_set_ids:
        category, reason, action = "blocked_conditional", MPLUS_REASON, MPLUS_ACTION
    elif family == "easyflow":
        category, reason, action = "blocked_partial", EASYFLOW_REASON, EASYFLOW_ACTION
    elif ready_for_benchmark and ready_for_customer_view and complete:
        category, reason, action = "customer_view_ready", READY_REASON, READY_ACTION
    elif not complete:
        category = "blocked_partial"
        reason = "Production assembly has incomplete technical data."
        action = "Complete and source the missing technical fields before customer publication."
    else:
        category = "not_applicable"
        reason = "No customer-view publication policy applies to this row in the stable dataset."
        action = "Review manually before changing any customer-view flag."

    return {
        "source_sheet": source_sheet,
        "source_row_number": int(row.name) + 2 if isinstance(row.name, int) else "",
        "record_id": row_id,
        "assembled_family": family,
        "category": category,
        "policy_reason": reason,
        "recommended_next_action": action,
        "ready_for_benchmark": ready_for_benchmark,
        "ready_for_customer_view": ready_for_customer_view,
        "customer_view_enabled": customer_view_enabled,
        "complete_technical_data": complete,
    }


def _diagnostic_rows(frame: pd.DataFrame, source_sheet: str) -> list[dict[str, Any]]:
    if source_sheet == "Eplus_Compatible_Grate_Evidence":
        family, reason, action = "showerdrain_eplus", EPLUS_REASON, EPLUS_ACTION
    else:
        family, reason, action = "showerdrain_b", BLINE_REASON, BLINE_ACTION

    rows: list[dict[str, Any]] = []
    for index, row in frame.iterrows():
        record_id = ""
        for column in ("set_id", "product_id", "article_number", "source_url"):
            record_id = _text(row.get(column))
            if record_id:
                break
        rows.append({
            "source_sheet": source_sheet,
            "source_row_number": int(index) + 2 if isinstance(index, int) else "",
            "record_id": record_id,
            "assembled_family": family,
            "category": "diagnostic_only_not_product",
            "policy_reason": reason,
            "recommended_next_action": action,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "customer_view_enabled": False,
            "complete_technical_data": False,
        })
    return rows


def build_customer_view_readiness_report(
    sheets: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    """Classify every final assembly/detail and E+/B-line diagnostic evidence row.

    All inputs are copied before evaluation.  The function has no write path and never
    updates a source frame or policy flag.
    """
    frames = {name: pd.DataFrame(sheets.get(name)).copy(deep=True) for name in INPUT_SHEETS}
    product_policy = _product_policy_lookup(frames["Products"])
    conditional = frames["Conditional_Technical_Values"]
    conditional_set_ids = {
        _text(value) for value in _column(conditional, "set_id") if _text(value)
    }

    records: list[dict[str, Any]] = []
    for source_sheet in ASSEMBLY_SHEETS:
        for _, row in frames[source_sheet].iterrows():
            records.append(
                _classify_assembly(
                    row,
                    source_sheet=source_sheet,
                    product_policy=product_policy,
                    conditional_set_ids=conditional_set_ids,
                )
            )
    for source_sheet in ("Eplus_Compatible_Grate_Evidence", "Bline_Source_Evidence"):
        records.extend(_diagnostic_rows(frames[source_sheet], source_sheet))

    report = pd.DataFrame.from_records(records)
    if report.empty:
        return pd.DataFrame(columns=(
            "source_sheet", "source_row_number", "record_id", "assembled_family",
            "category", "policy_reason", "recommended_next_action",
            "ready_for_benchmark", "ready_for_customer_view", "customer_view_enabled",
            "complete_technical_data",
        ))
    report["category"] = pd.Categorical(report["category"], categories=CATEGORIES, ordered=True)
    return report.sort_values(
        ["category", "assembled_family", "source_sheet", "record_id"], kind="stable"
    ).reset_index(drop=True)


def load_report_sheets(path: str | Path) -> dict[str, pd.DataFrame]:
    """Load only report inputs from an existing workbook without modifying it."""
    with pd.ExcelFile(path, engine="openpyxl") as workbook:
        missing = [name for name in INPUT_SHEETS if name not in workbook.sheet_names]
        if missing:
            raise ValueError(f"Workbook is missing required report sheets: {', '.join(missing)}")
        return {name: pd.read_excel(workbook, sheet_name=name) for name in INPUT_SHEETS}


def print_report(report: pd.DataFrame, *, source: str) -> None:
    print("ACO customer-view readiness diagnostic")
    print(f"Source: {source}")
    print("Mode: read-only policy evaluation; no frames, flags, or XLSX outputs are modified.")

    print("\nCategory counts by source sheet:")
    counts = report.groupby(["source_sheet", "category"], observed=True).size()
    for (sheet, category), count in counts.items():
        print(f"- {sheet} / {category}: {count}")

    print("\nAssembly family decisions (Final_Assemblies rows):")
    assemblies = report[report["source_sheet"].eq("Final_Assemblies")]
    for family, rows in assemblies.groupby("assembled_family", sort=True):
        categories = ", ".join(
            f"{category}={count}"
            for category, count in rows.groupby("category", observed=True).size().items()
        )
        first = rows.iloc[0]
        print(f"- {family}: {categories}")
        print(f"  Reason: {first['policy_reason']}")
        print(f"  Next action: {first['recommended_next_action']}")

    print("\nDiagnostic-only groups:")
    diagnostics = report[report["category"].eq("diagnostic_only_not_product")]
    for family, rows in diagnostics.groupby("assembled_family", sort=True):
        first = rows.iloc[0]
        print(f"- {family}: {len(rows)} evidence rows")
        print(f"  Reason: {first['policy_reason']}")
        print(f"  Next action: {first['recommended_next_action']}")

    print("\nPolicy result: customer-view flags remain unchanged; no automatic promotion recommended.")


def _report_existing_workbook(path: Path) -> pd.DataFrame:
    return build_customer_view_readiness_report(load_report_sheets(path))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--xlsx",
        type=Path,
        help="Read an existing canonical XLSX instead of building a temporary validated export.",
    )
    args = parser.parse_args(argv)

    if args.xlsx is not None:
        report = _report_existing_workbook(args.xlsx)
        print_report(report, source=str(args.xlsx))
        return 0

    template = REPO_ROOT / "data" / "templates" / "benchmark_template.xlsx"
    with TemporaryDirectory(prefix="aco-customer-readiness-") as temporary:
        workbook = Path(temporary) / "benchmark_output.xlsx"
        try:
            export_canonical_aco_workbook(template, workbook, default_config())
        except AppExportValidationError as exc:
            print(f"ERROR: unable to build the stable canonical report input: {exc}", file=sys.stderr)
            print(
                "Use --xlsx with an existing validator-approved canonical workbook, or restore "
                "the canonical source inputs before retrying.",
                file=sys.stderr,
            )
            return 1
        report = _report_existing_workbook(workbook)
    print_report(report, source="temporary validator-approved canonical export")
    return 0


if __name__ == "__main__":
    sys.exit(main())
