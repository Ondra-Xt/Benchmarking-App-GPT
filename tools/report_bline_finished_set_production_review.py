"""Read-only production review for ACO ShowerDrain B integral finished-set evidence.

The report evaluates whether the eight diagnostic B-line article rows are technically
eligible to enter a *manual* direct-product production review. It never promotes rows,
changes policy flags, or writes to the inspected XLSX. B-line must remain modeled as an
integral article-level finished set rather than as a generated base_x_grate assembly.
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

EXPECTED_ARTICLES = frozenset({
    "9010.78.70",
    "9010.78.71",
    "9010.78.72",
    "9010.78.73",
    "3018172",
    "3018173",
    "3018174",
    "3018175",
})
EXPECTED_SHEET_COUNTS = {
    "Products": 80,
    "Comparison": 80,
    "Scoring_Field_Coverage": 80,
    "Candidates_All": 118,
    "Components": 100,
    "BOM_Options": 251,
    "Final_Assemblies": 62,
    "Final_Set_Details": 62,
    "Article_Variants": 76,
    "Mplus_Compound_Mappings": 4,
    "Eplus_Proposal_Mappings": 3,
    "Eplus_Compatible_Grate_Evidence": 3,
    "Cplus_Compatible_Grate_Evidence": 30,
    "Bline_Source_Evidence": 8,
    "Conditional_Technical_Values": 8,
    "Scoring_Scenarios": 3,
    "Comparison_flow_head_10mm": 80,
    "Comparison_flow_head_20mm": 80,
}
REQUIRED_SHEETS = tuple(EXPECTED_SHEET_COUNTS)
PRODUCTION_SHEETS = (
    "Products",
    "Comparison",
    "BOM_Options",
    "Final_Assemblies",
    "Final_Set_Details",
)

ELIGIBLE = "eligible_for_finished_set_production_review"
BLOCKED = "blocked_for_finished_set_production"
MANUAL_APPROVAL = "requires_manual_approval"
EASYFLOW_MISSING_TECHNICAL_FIELDS = (
    "flow_rate_lps,height_adj_min_mm,height_adj_max_mm"
)
RECOMMENDED_NEXT_ACTION = (
    "Approve direct finished-set product modelling before promotion; do not generate "
    "body × grate or base_x_grate assemblies."
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


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "1.0", "true", "yes", "y"}


def _falsey(value: Any) -> bool:
    if isinstance(value, bool):
        return not value
    return _text(value).lower() in {"0", "0.0", "false", "no", "n"}


def _empty(value: Any) -> bool:
    return _text(value) == ""


def _numeric_equal(value: Any, expected: float | int) -> bool:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return bool(pd.notna(numeric) and float(numeric) == float(expected))


def _series(frame: pd.DataFrame, column: str, default: Any = "") -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)


def _bline_production_rows(frame: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """Select actual B-line production rows while excluding source-family catalog rows.

    Products and Comparison can legitimately contain a discovered family-level B row.
    Such a row is not one of the eight direct finished-set products and must not be
    treated as a promotion. Production is identified by an assembled B identifier or
    by an explicit known finished-set article. BOM rows additionally count explicit
    B-line body/grate generation so that prohibited base_x_grate modelling is visible.
    """
    frame = frame.copy(deep=True)
    assembled_identity = pd.Series(False, index=frame.index)
    for column in ("product_id", "assembled_product_id", "set_id"):
        values = _series(frame, column).fillna("").astype(str).str.strip().str.lower()
        assembled_identity |= values.str.startswith("aco-assembled-showerdrain-b-")
        assembled_identity |= values.str.startswith("aco-assembled-showerdrain_b-")

    if sheet_name in {"Final_Assemblies", "Final_Set_Details"}:
        families = _series(frame, "assembled_family").map(lambda value: _text(value).lower())
        return frame[assembled_identity | families.eq("showerdrain_b")].copy()

    # Raw connector rows may expose a family-level ``article_number``. Only the
    # explicit finished-set production field identifies a direct promoted product.
    known_article = _series(frame, "product_article_number").map(_text).isin(EXPECTED_ARTICLES)

    if sheet_name == "BOM_Options":
        families = pd.Series(False, index=frame.index)
        for column in ("product_family", "parent_family", "option_family", "assembled_family"):
            families |= _series(frame, column).map(lambda value: _text(value).lower()).eq("showerdrain_b")
        models = _series(frame, "assembly_model").map(lambda value: _text(value).lower())
        roles = _series(frame, "option_role").map(lambda value: _text(value).lower())
        generated_bom = families & (models.eq("base_x_grate") | roles.eq("grate"))
        return frame[assembled_identity | known_article | generated_bom].copy()

    return frame[assembled_identity | known_article].copy()


def _family_rows(frame: pd.DataFrame, family: str) -> pd.DataFrame:
    values = _series(frame, "assembled_family").fillna("").astype(str).str.strip().str.lower()
    if not values.ne("").any():
        values = _series(frame, "product_family").fillna("").astype(str).str.strip().str.lower()
    return frame[values.eq(family)].copy()


def _row_checks(row: pd.Series) -> dict[str, bool]:
    return {
        "known_finished_set_article": _text(row.get("product_article_number")) in EXPECTED_ARTICLES,
        "body_article_number_empty": _empty(row.get("body_article_number")),
        "grate_article_number_empty": _empty(row.get("grate_article_number")),
        "integral_finished_set_model": _text(row.get("assembly_model")) == "integral_all_in_one_set",
        "explicit_article_level_evidence": (
            _text(row.get("compatibility_evidence_type"))
            == "explicit_article_level_integral_complete_set"
        ),
        "high_confidence": _text(row.get("compatibility_confidence")).lower() == "high",
        "article_level_compatibility_found": _truthy(row.get("article_level_compatibility_found")),
        "unconditional_flow_empty": _empty(row.get("flow_rate_lps")),
        "flow_rate_10mm_is_0_40": _numeric_equal(row.get("flow_rate_10mm_lps"), 0.40),
        "flow_rate_20mm_is_0_46": _numeric_equal(row.get("flow_rate_20mm_lps"), 0.46),
        "water_seal_is_30mm": _numeric_equal(row.get("water_seal_mm"), 30),
        "outlet_is_dn50": _text(row.get("outlet_dn")).upper() == "DN50",
        "installation_height_is_80mm": _numeric_equal(row.get("installation_height_mm"), 80),
        "height_adjustment_min_empty": _empty(row.get("height_adj_min_mm")),
        "height_adjustment_max_empty": _empty(row.get("height_adj_max_mm")),
        "safe_to_generate_false": _falsey(row.get("safe_to_generate")),
        "ready_for_benchmark_false": _falsey(row.get("ready_for_benchmark")),
        "ready_for_customer_view_false": _falsey(row.get("ready_for_customer_view")),
    }


def build_bline_finished_set_production_review(
    sheets: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    """Return one review row per B-line evidence row without mutating inputs."""
    missing = [name for name in REQUIRED_SHEETS if name not in sheets]
    if missing:
        raise ValueError(f"Missing required workbook sheets: {', '.join(missing)}")

    frames = {name: pd.DataFrame(sheets[name]).copy(deep=True) for name in REQUIRED_SHEETS}
    evidence = frames["Bline_Source_Evidence"]
    article_counts = _series(evidence, "product_article_number").map(_text).value_counts()

    records: list[dict[str, Any]] = []
    for index, row in evidence.iterrows():
        checks = _row_checks(row)
        article = _text(row.get("product_article_number"))
        checks["finished_set_article_unique"] = article_counts.get(article, 0) == 1
        passed = all(checks.values())
        failed = [name for name, result in checks.items() if not result]
        records.append({
            "source_row_number": int(index) + 2 if isinstance(index, int) else "",
            "evidence_id": _text(row.get("evidence_id")),
            "product_article_number": article,
            "variant_condition": _text(row.get("variant_condition")),
            "classification": ELIGIBLE if passed else BLOCKED,
            "production_gate": MANUAL_APPROVAL if passed else BLOCKED,
            "blocking_reasons": ",".join(failed),
            "recommended_next_action": RECOMMENDED_NEXT_ACTION,
            **checks,
        })
    return pd.DataFrame.from_records(records)


def workbook_diagnostics(
    sheets: Mapping[str, pd.DataFrame], review: pd.DataFrame,
) -> dict[str, Any]:
    """Summarize stable counts and protected family policy state."""
    counts = {name: len(sheets[name]) for name in REQUIRED_SHEETS}
    bline_counts = {
        f"Bline_{name}": len(_bline_production_rows(sheets[name], name))
        for name in PRODUCTION_SHEETS
    }

    final = sheets["Final_Assemblies"]
    details = sheets["Final_Set_Details"]
    products = sheets["Products"]
    comparison = sheets["Comparison"]
    mplus = _family_rows(final, "showerdrain_mplus")
    easyflow = _family_rows(final, "easyflow")
    eplus_evidence = sheets["Eplus_Compatible_Grate_Evidence"]
    cplus_details = _family_rows(details, "showerdrain_cplus")

    ready_count = int(_series(details, "ready_for_customer_view", False).map(_truthy).sum())
    mplus_blocked = bool(
        len(mplus) == 4
        and _series(mplus, "ready_for_benchmark", None).map(_falsey).all()
        and _series(mplus, "ready_for_customer_view", None).map(_falsey).all()
    )
    easyflow_details = _family_rows(details, "easyflow")
    easyflow_assemblies_blocked = bool(
        len(easyflow) == 2
        and _series(easyflow, "data_quality_status")
        .map(lambda value: _text(value).lower()).eq("partial").all()
        and _series(easyflow, "is_complete_technical_data", None).map(_falsey).all()
        and _series(easyflow, "missing_technical_fields").map(_text)
        .eq(EASYFLOW_MISSING_TECHNICAL_FIELDS).all()
    )
    easyflow_details_blocked = bool(
        len(easyflow_details) == len(easyflow)
        and len(easyflow_details) == 2
        and _series(easyflow_details, "data_quality_status")
        .map(lambda value: _text(value).lower()).eq("partial").all()
        and _series(easyflow_details, "ready_for_benchmark", None).map(_falsey).all()
        and _series(easyflow_details, "ready_for_customer_view", None).map(_falsey).all()
    )
    easyflow_blocked = easyflow_assemblies_blocked and easyflow_details_blocked
    eplus_diagnostic_only = bool(
        len(eplus_evidence) == 3
        and len(_family_rows(products, "showerdrain_eplus")) == 0
        and len(_family_rows(comparison, "showerdrain_eplus")) == 0
        and len(_family_rows(final, "showerdrain_eplus")) == 0
        and len(_family_rows(details, "showerdrain_eplus")) == 0
        and _series(eplus_evidence, "ready_for_benchmark", False).map(_falsey).all()
        and _series(eplus_evidence, "ready_for_customer_view", False).map(_falsey).all()
    )
    cplus_customer_state_unchanged = bool(
        len(cplus_details) == 30
        and _series(cplus_details, "ready_for_customer_view", False).map(_truthy).all()
    )

    return {
        **counts,
        **bline_counts,
        "Final_Set_Details_ready_for_customer_view_true": ready_count,
        "Cplus_customer_approved_state_unchanged": cplus_customer_state_unchanged,
        "Mplus_blocked": mplus_blocked,
        "Easyflow_blocked": easyflow_blocked,
        "Eplus_diagnostic_only": eplus_diagnostic_only,
        "review_rows": len(review),
        "eligible_review_rows": int(review.get("classification", pd.Series(dtype=str)).eq(ELIGIBLE).sum()),
        "blocked_review_rows": int(review.get("classification", pd.Series(dtype=str)).eq(BLOCKED).sum()),
    }


def load_review_sheets(path: str | Path) -> dict[str, pd.DataFrame]:
    """Load review inputs from an existing workbook through a read-only code path."""
    with pd.ExcelFile(path, engine="openpyxl") as workbook:
        missing = [name for name in REQUIRED_SHEETS if name not in workbook.sheet_names]
        if missing:
            raise ValueError(f"Missing required workbook sheets: {', '.join(missing)}")
        return {name: pd.read_excel(workbook, sheet_name=name) for name in REQUIRED_SHEETS}


def _baseline_ok(diagnostics: Mapping[str, Any]) -> bool:
    return all(diagnostics[name] == expected for name, expected in EXPECTED_SHEET_COUNTS.items()) and all((
        diagnostics["Bline_Products"] == 0,
        diagnostics["Bline_Comparison"] == 0,
        diagnostics["Bline_BOM_Options"] == 0,
        diagnostics["Bline_Final_Assemblies"] == 0,
        diagnostics["Bline_Final_Set_Details"] == 0,
        diagnostics["Final_Set_Details_ready_for_customer_view_true"] == 56,
        diagnostics["Cplus_customer_approved_state_unchanged"],
        diagnostics["Mplus_blocked"],
        diagnostics["Easyflow_blocked"],
        diagnostics["Eplus_diagnostic_only"],
    ))


def _review_ok(review: pd.DataFrame) -> bool:
    return bool(
        len(review) == 8
        and set(review["product_article_number"]) == EXPECTED_ARTICLES
        and review["classification"].eq(ELIGIBLE).all()
        and review["production_gate"].eq(MANUAL_APPROVAL).all()
    )


def print_report(path: Path, review: pd.DataFrame, diagnostics: Mapping[str, Any]) -> None:
    print("ACO ShowerDrain B finished-set production review")
    print(f"Workbook: {path}")
    print("Mode: read-only diagnostic; no workbook rows, assemblies, or policy flags were changed")
    print("Modelling rule: direct integral finished-set articles only; base_x_grate is prohibited")
    print("\nBaseline diagnostics:")
    for name, value in diagnostics.items():
        print(f"- {name}: {value}")
    print("\nEvidence-row review:")
    for row in review.itertuples(index=False):
        suffix = f"; blocking={row.blocking_reasons}" if row.blocking_reasons else ""
        print(
            f"- {row.product_article_number}: {row.classification}; "
            f"production_gate={row.production_gate}{suffix}"
        )
    overall = MANUAL_APPROVAL if _baseline_ok(diagnostics) and _review_ok(review) else BLOCKED
    print(f"\nOVERALL: {overall}")
    print(f"Production gate: {overall}")
    print(f"Recommended next action: {RECOMMENDED_NEXT_ACTION}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xlsx", required=True, type=Path, help="Existing canonical benchmark workbook")
    parser.add_argument(
        "--csv-out",
        type=Path,
        help="Optional separate diagnostic CSV output; the input XLSX remains unchanged",
    )
    args = parser.parse_args(argv)
    try:
        sheets = load_review_sheets(args.xlsx)
        review = build_bline_finished_set_production_review(sheets)
        diagnostics = workbook_diagnostics(sheets, review)
    except (FileNotFoundError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.csv_out is not None:
        args.csv_out.parent.mkdir(parents=True, exist_ok=True)
        review.to_csv(args.csv_out, index=False)
    print_report(args.xlsx, review, diagnostics)
    return 0 if _baseline_ok(diagnostics) and _review_ok(review) else 1


if __name__ == "__main__":
    raise SystemExit(main())
