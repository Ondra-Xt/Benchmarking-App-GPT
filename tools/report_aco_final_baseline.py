"""Read-only final audit for the frozen canonical ACO benchmark baseline."""
from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.customer_scenario_view import (
    FLOW_HEAD_10MM,
    FLOW_HEAD_20MM,
    NO_SCENARIO_SELECTED,
    build_customer_scenario_projection,
)

STABLE = "ACO_BASELINE_STABLE"
UNSTABLE = "ACO_BASELINE_UNSTABLE"

EXPECTED_SHEET_COUNTS = {
    "Products": 88,
    "Comparison": 88,
    "Scoring_Field_Coverage": 88,
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
    "Conditional_Technical_Values": 24,
    "Scoring_Scenarios": 3,
    "Comparison_flow_head_10mm": 88,
    "Comparison_flow_head_20mm": 88,
}
EXPECTED_FINAL_FAMILY_COUNTS = {
    "easyflow": 2,
    "easyflowplus": 6,
    "showerdrain_splus": 16,
    "showerdrain_c": 4,
    "showerdrain_cplus": 30,
    "showerdrain_mplus": 4,
    "showerdrain_b": 0,
    "showerdrain_eplus": 0,
}
EASYFLOW_ARTICLES = {"2500.00.00", "2500.05.00", "2500.55.00"}
CPLUS_BASE_HYDRAULICS = {
    "aco-showerdrain-cplus-standard-h92": (0.91, 50.0, "DN50", 80.0, 128.0),
    "aco-showerdrain-cplus-low-h69": (0.62, 25.0, "DN50", 57.0, 128.0),
}
BLINE_ARTICLES = {
    "9010.78.70", "9010.78.71", "9010.78.72", "9010.78.73",
    "3018172", "3018173", "3018174", "3018175",
}


@dataclass(frozen=True)
class AuditCheck:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class AuditReport:
    checks: tuple[AuditCheck, ...]
    sheet_counts: Mapping[str, int]
    family_counts: Mapping[str, int]
    customer_counts: Mapping[str, int]
    workbook_sha256_before: str = ""
    workbook_sha256_after: str = ""

    @property
    def stable(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def overall(self) -> str:
        return STABLE if self.stable else UNSTABLE


def _text(value: Any) -> str:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    return str(value).strip()


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype="string")
    return frame[column].map(_text).astype("string")


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return False
    return _text(value).lower() in {"true", "1", "yes", "y"}


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    return frame[column].map(_bool)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(float("nan"), index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _family(frame: pd.DataFrame, *, assembled: bool = False) -> pd.Series:
    primary = "assembled_family" if assembled else "product_family"
    values = _series(frame, primary).str.lower()
    if values.eq("").all():
        values = _series(frame, "family").str.lower()
    return values


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_workbook(path: str | Path) -> dict[str, pd.DataFrame]:
    """Load the protected sheets from an existing workbook without writing it."""
    workbook_path = Path(path)
    with pd.ExcelFile(workbook_path, engine="openpyxl") as workbook:
        missing = [name for name in EXPECTED_SHEET_COUNTS if name not in workbook.sheet_names]
        if missing:
            raise ValueError(f"Missing required workbook sheets: {', '.join(missing)}")
        return {
            name: pd.read_excel(workbook, sheet_name=name)
            for name in EXPECTED_SHEET_COUNTS
        }


def _ready_count(frame: pd.DataFrame) -> int:
    return int(_bool_series(frame, "ready_for_customer_view").sum())


def _scenario_ready_count(frame: pd.DataFrame, family: str) -> int:
    rows = frame[_family(frame).eq(family)]
    return int(_bool_series(rows, "customer_ready_for_selected_scenario").sum())


def _add(checks: list[AuditCheck], name: str, passed: bool, detail: str) -> None:
    checks.append(AuditCheck(name, bool(passed), detail))


def audit_frames(sheets: Mapping[str, pd.DataFrame]) -> AuditReport:
    """Audit already-loaded frames; inputs are copied and never mutated."""
    frames = {name: frame.copy(deep=True) for name, frame in sheets.items()}
    checks: list[AuditCheck] = []
    sheet_counts = {name: len(frames.get(name, pd.DataFrame())) for name in EXPECTED_SHEET_COUNTS}
    for name, expected in EXPECTED_SHEET_COUNTS.items():
        actual = sheet_counts[name]
        _add(checks, f"sheet_count:{name}", actual == expected, f"actual={actual} expected={expected}")

    products = frames.get("Products", pd.DataFrame())
    assemblies = frames.get("Final_Assemblies", pd.DataFrame())
    details = frames.get("Final_Set_Details", pd.DataFrame())
    bom = frames.get("BOM_Options", pd.DataFrame())
    variants = frames.get("Article_Variants", pd.DataFrame())
    conditions = frames.get("Conditional_Technical_Values", pd.DataFrame())
    eplus_proposals = frames.get("Eplus_Proposal_Mappings", pd.DataFrame())
    eplus_evidence = frames.get("Eplus_Compatible_Grate_Evidence", pd.DataFrame())
    cplus_evidence = frames.get("Cplus_Compatible_Grate_Evidence", pd.DataFrame())

    assembly_families = _family(assemblies, assembled=True)
    family_counts = {
        family: int(assembly_families.eq(family).sum())
        for family in EXPECTED_FINAL_FAMILY_COUNTS
    }
    for family, expected in EXPECTED_FINAL_FAMILY_COUNTS.items():
        _add(checks, f"final_family_count:{family}", family_counts[family] == expected,
             f"actual={family_counts[family]} expected={expected}")

    detail_families = _family(details, assembled=True)
    product_families = _family(products)
    customer_counts = {
        "canonical_final_set_details": _ready_count(details),
        "cplus": _ready_count(details[detail_families.eq("showerdrain_cplus")]),
        "easyflow": _ready_count(details[detail_families.eq("easyflow")]),
        "mplus": _ready_count(details[detail_families.eq("showerdrain_mplus")]),
        "bline": _ready_count(products[product_families.eq("showerdrain_b")]),
        "eplus": _ready_count(products[product_families.eq("showerdrain_eplus")]),
    }
    expected_customer = {
        "canonical_final_set_details": 56, "cplus": 30, "easyflow": 0,
        "mplus": 0, "bline": 0, "eplus": 0,
    }
    for name, expected in expected_customer.items():
        actual = customer_counts[name]
        _add(checks, f"canonical_customer_ready:{name}", actual == expected,
             f"actual={actual} expected={expected}")

    # Easyflow must remain a partial, fail-closed two-assembly family with all
    # three candidates retained and no selected article or protected scalar.
    easy_a = assemblies[assembly_families.eq("easyflow")]
    easy_d = details[detail_families.eq("easyflow")]
    easy_v = variants[
        _family(variants).eq("easyflow")
        & _series(variants, "variant_type").str.lower().eq("candidate_body_variant")
    ]
    found_articles = set(_series(easy_v, "article_number")) - {""}
    selected_article_columns = [
        column for column in ("base_article_number", "article_number", "selected_article_number")
        if column in easy_d.columns
    ]
    no_selected_article = all(_series(easy_d, column).eq("").all() for column in selected_article_columns)
    empty_easy_scalars = all(_series(easy_a, column).eq("").all() for column in (
        "flow_rate_lps", "height_adj_min_mm", "height_adj_max_mm"
    ))
    _add(checks, "easyflow_partial_blocked", len(easy_a) == 2
         and _series(easy_a, "data_quality_status").str.lower().eq("partial").all()
         and not _bool_series(easy_a, "ready_for_benchmark").any()
         and not _bool_series(easy_a, "ready_for_customer_view").any(),
         f"assemblies={len(easy_a)}")
    _add(checks, "easyflow_protected_scalars_empty", empty_easy_scalars,
         "flow_rate_lps/height_adj_min_mm/height_adj_max_mm must be empty")
    _add(checks, "easyflow_candidate_articles", found_articles == EASYFLOW_ARTICLES,
         f"actual={sorted(found_articles)} expected={sorted(EASYFLOW_ARTICLES)}")
    _add(checks, "easyflow_no_silent_article_selection", no_selected_article,
         f"selected_columns={selected_article_columns}")

    # Complete unconditional families.
    for family, expected in (("easyflowplus", 6), ("showerdrain_splus", 16), ("showerdrain_c", 4)):
        rows = assemblies[assembly_families.eq(family)]
        family_details = details[detail_families.eq(family)]
        passed = (len(rows) == expected and _bool_series(rows, "ready_for_benchmark").all()
                  and _bool_series(family_details, "ready_for_customer_view").all())
        _add(checks, f"complete_customer_family:{family}", passed,
             f"assemblies={len(rows)} customer_ready={_ready_count(family_details)} expected={expected}")

    # C+ approved matrix scope, hydraulics, BOM, and customer state.
    cplus_a = assemblies[assembly_families.eq("showerdrain_cplus")]
    cplus_d = details[detail_families.eq("showerdrain_cplus")]
    cplus_bom = bom[_family(bom).eq("showerdrain_cplus")]
    compatible_bom = cplus_bom[_series(cplus_bom, "option_type").str.lower().eq("compatible_grate")]
    cplus_base_ids = set(_series(cplus_evidence, "base_id")) - {""}
    cplus_no_tile = not pd.concat([
        _series(cplus_evidence, "grate_id"), _series(cplus_evidence, "grate_article_number"),
        _series(cplus_evidence, "grate_name"), _series(compatible_bom, "component_id"),
    ], ignore_index=True).str.lower().str.contains("tile").any()
    protected_hydraulics = True
    for base_id, expected_values in CPLUS_BASE_HYDRAULICS.items():
        rows = cplus_evidence[_series(cplus_evidence, "base_id").eq(base_id)]
        actual = (
            _numeric(rows, "flow_rate_lps"), _numeric(rows, "water_seal_mm"),
            _series(rows, "outlet_dn"), _numeric(rows, "height_adj_min_mm"),
            _numeric(rows, "height_adj_max_mm"),
        )
        protected_hydraulics &= bool(len(rows) == 15
            and actual[0].round(2).eq(expected_values[0]).all()
            and actual[1].eq(expected_values[1]).all()
            and actual[2].eq(expected_values[2]).all()
            and actual[3].eq(expected_values[3]).all()
            and actual[4].eq(expected_values[4]).all())
    _add(checks, "cplus_approved_scope", len(cplus_a) == 30 and len(cplus_evidence) == 30
         and len(compatible_bom) == 30 and cplus_base_ids == set(CPLUS_BASE_HYDRAULICS)
         and cplus_no_tile, f"assemblies={len(cplus_a)} evidence={len(cplus_evidence)} compatible_bom={len(compatible_bom)}")
    _add(checks, "cplus_protected_hydraulics", protected_hydraulics,
         "expected 15 standard H92 rows and 15 low H69 rows with protected values")
    _add(checks, "cplus_customer_approved_enabled", _ready_count(cplus_d) == 30
         and _bool_series(cplus_a, "ready_for_benchmark").all()
         and _bool_series(cplus_a, "customer_view_enabled").all(),
         f"customer_ready={_ready_count(cplus_d)}")

    # M+ and B-line are conditional only: no canonical/default scalar and exact
    # scenario-specific runtime readiness at 10 mm and 20 mm.
    mplus_products = products[product_families.eq("showerdrain_mplus")]
    bline_products = products[product_families.eq("showerdrain_b")]
    for label, rows, expected, model in (
        ("mplus", mplus_products, 4, "channel_body_x_drain_body_x_grate"),
        ("bline", bline_products, 8, "integral_all_in_one_set"),
    ):
        scalar_empty = _series(rows, "flow_rate_lps").eq("").all()
        default_empty = _series(rows, "selected_default_flow_rate_lps").eq("").all()
        _add(checks, f"{label}_zero_scalar_defaults", len(rows) == expected and scalar_empty and default_empty,
             f"rows={len(rows)} scalar_filled={int(_series(rows, 'flow_rate_lps').ne('').sum())} default_filled={int(_series(rows, 'selected_default_flow_rate_lps').ne('').sum())}")
        _add(checks, f"{label}_assembly_model", len(rows) == expected
             and _series(rows, "assembly_model").eq(model).all(),
             f"expected={model}")

    bline_ids = set(_series(bline_products, "product_article_number")) - {""}
    bline_bom = bom[_family(bom).eq("showerdrain_b")]
    _add(checks, "bline_direct_integral_finished_sets", len(bline_products) == 8
         and bline_ids == BLINE_ARTICLES and len(bline_bom) == 0
         and family_counts["showerdrain_b"] == 0
         and _series(bline_products, "body_article_number").eq("").all()
         and _series(bline_products, "grate_article_number").eq("").all(),
         f"products={len(bline_products)} bom={len(bline_bom)} final_assemblies={family_counts['showerdrain_b']}")

    projections = {
        "none": build_customer_scenario_projection(products.copy(deep=True), conditions.copy(deep=True), NO_SCENARIO_SELECTED),
        "10mm": build_customer_scenario_projection(products.copy(deep=True), conditions.copy(deep=True), FLOW_HEAD_10MM),
        "20mm": build_customer_scenario_projection(products.copy(deep=True), conditions.copy(deep=True), FLOW_HEAD_20MM),
    }
    for family, label, expected_rows in (("showerdrain_mplus", "mplus", 4), ("showerdrain_b", "bline", 8)):
        none_rows = projections["none"][_family(projections["none"]).eq(family)]
        ready_10 = _scenario_ready_count(projections["10mm"], family)
        ready_20 = _scenario_ready_count(projections["20mm"], family)
        flow_10 = _numeric(projections["10mm"][_family(projections["10mm"]).eq(family)], "flow_rate_lps")
        flow_20 = _numeric(projections["20mm"][_family(projections["20mm"]).eq(family)], "flow_rate_lps")
        customer_counts[f"runtime_{label}_10mm"] = ready_10
        customer_counts[f"runtime_{label}_20mm"] = ready_20
        _add(checks, f"runtime_{label}_no_selection_blocked", len(none_rows) == expected_rows
             and not _bool_series(none_rows, "customer_ready_for_selected_scenario").any()
             and _series(none_rows, "flow_rate_lps").eq("").all(), f"rows={len(none_rows)}")
        _add(checks, f"runtime_{label}_10mm", ready_10 == expected_rows and flow_10.round(2).eq(0.40).all(),
             f"ready={ready_10} expected={expected_rows} flow=0.40")
        _add(checks, f"runtime_{label}_20mm", ready_20 == expected_rows and flow_20.round(2).eq(0.46).all(),
             f"ready={ready_20} expected={expected_rows} flow=0.46")

    # E+ remains proposals/evidence only and must not leak into production.
    eplus_bom = bom[_family(bom).eq("showerdrain_eplus")]
    eplus_no_matrix = _series(eplus_evidence, "article_level_compatibility_found").map(
        lambda value: not _bool(value)
    ).all()
    eplus_diag = _series(eplus_evidence, "production_status_note").str.lower().str.contains("diagnostic-only").all()
    _add(checks, "eplus_diagnostic_only", len(eplus_proposals) == 3 and len(eplus_evidence) == 3
         and family_counts["showerdrain_eplus"] == 0 and len(eplus_bom) == 0
         and eplus_no_matrix and eplus_diag
         and not _bool_series(eplus_evidence, "ready_for_customer_view").any(),
         f"proposals={len(eplus_proposals)} evidence={len(eplus_evidence)} production_assemblies={family_counts['showerdrain_eplus']}")

    return AuditReport(tuple(checks), sheet_counts, family_counts, customer_counts)


def audit_workbook(path: str | Path) -> AuditReport:
    workbook_path = Path(path)
    before = _sha256(workbook_path)
    report = audit_frames(load_workbook(workbook_path))
    after = _sha256(workbook_path)
    unchanged = before == after
    checks = report.checks + (AuditCheck(
        "workbook_byte_for_byte_unchanged", unchanged,
        f"before={before} after={after}",
    ),)
    return AuditReport(checks, report.sheet_counts, report.family_counts,
                       report.customer_counts, before, after)


def format_report(path: Path, report: AuditReport) -> str:
    lines = [
        "ACO final canonical baseline audit (read-only)",
        f"workbook: {path}",
        "mode: read-only; no production frames, policies, schemas, flags, or values are changed",
        "",
        "protected_counts:",
    ]
    lines.extend(f"  - {name}: {count}" for name, count in report.sheet_counts.items())
    lines.append("family_counts:")
    lines.extend(f"  - {name}: {count}" for name, count in report.family_counts.items())
    lines.append("customer_view_states:")
    lines.extend(f"  - {name}: {count}" for name, count in report.customer_counts.items())
    lines.append("checks:")
    lines.extend(
        f"  - {'PASS' if check.passed else 'FAIL'} {check.name}: {check.detail}"
        for check in report.checks
    )
    lines.extend([
        f"OVERALL: {report.overall}",
        "unresolved_nonblocking:",
        "  - Easyflow article-level attribution",
        "  - E+ explicit compatibility evidence",
        "recommended_next_step:",
        "  - freeze ACO baseline and begin next manufacturer integration",
    ])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xlsx", required=True, type=Path, help="Canonical ACO XLSX workbook")
    args = parser.parse_args(argv)
    if not args.xlsx.is_file():
        print(f"ERROR: XLSX file does not exist: {args.xlsx}", file=sys.stderr)
        return 2
    try:
        report = audit_workbook(args.xlsx)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(format_report(args.xlsx, report))
    return 0 if report.stable else 1


if __name__ == "__main__":
    raise SystemExit(main())
