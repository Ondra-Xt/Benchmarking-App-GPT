"""Read-only publication approval report for canonical ACO ShowerDrain C+ assemblies.

This diagnostic inspects an existing canonical workbook and never writes to it. A row is
approved for customer publication only when every protected C+ assembly, component,
hydraulic, evidence, and policy invariant is preserved and the production customer-view
flags reflect the granted manual approval.
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

CPLUS_PREFIX = "aco-assembled-showerdrain-cplus-"
CPLUS_FAMILY = "showerdrain_cplus"
EXPECTED_CPLUS_ASSEMBLIES = 30
EASYFLOW_MISSING_TECHNICAL_FIELDS = (
    "flow_rate_lps,height_adj_min_mm,height_adj_max_mm"
)

REQUIRED_SHEETS = (
    "Products",
    "Comparison",
    "BOM_Options",
    "Final_Assemblies",
    "Final_Set_Details",
    "Cplus_Compatible_Grate_Evidence",
)
OPTIONAL_READINESS_SHEETS = (
    "Customer_View_Readiness",
    "Customer_View_Readiness_Report",
)

PROTECTED_BASES: dict[str, dict[str, Any]] = {
    "aco-showerdrain-cplus-standard-h92": {
        "flow_rate_lps": 0.91,
        "water_seal_mm": 50,
        "outlet_dn": "DN50",
        "height_adj_min_mm": 80,
        "height_adj_max_mm": 128,
    },
    "aco-showerdrain-cplus-low-h69": {
        "flow_rate_lps": 0.62,
        "water_seal_mm": 25,
        "outlet_dn": "DN40",
        "height_adj_min_mm": 57,
        "height_adj_max_mm": 128,
    },
}
APPROVED_GRATE_ARTICLES = frozenset({
    "9010.88.61", "9010.88.62", "9010.88.63", "9010.88.64", "9010.88.66",
    "9010.88.68", "9010.88.69", "9010.88.70", "9010.88.71", "9010.88.73",
    "9010.88.89", "9010.88.90", "9010.88.91", "9010.88.92", "9010.88.94",
})
TECHNICAL_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
APPROVED = "approved_for_customer_publication"
BLOCKED = "blocked_for_customer_publication"
APPROVED_GATE = "approved"
RECOMMENDED_NEXT_ACTION = (
    "Retain customer-view publication for the approved 30 C+ production assemblies; "
    "do not expand the validated base or grate scope."
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
    text = _text(value).lower()
    return text in {"1", "1.0", "true", "yes", "y"}


def _falsey(value: Any) -> bool:
    if isinstance(value, bool):
        return not value
    text = _text(value).lower()
    return text in {"0", "0.0", "false", "no", "n"}


def _numeric_equal(value: Any, expected: float | int) -> bool:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return bool(pd.notna(numeric) and float(numeric) == float(expected))


def _first(row: pd.Series | None, *columns: str) -> str:
    if row is None:
        return ""
    for column in columns:
        value = _text(row.get(column))
        if value:
            return value
    return ""


def _cplus_rows(frame: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    frame = frame.copy()
    if sheet_name in {"Products", "Comparison"}:
        ids = frame.get("product_id", pd.Series("", index=frame.index)).fillna("").astype(str)
        return frame[ids.str.startswith(CPLUS_PREFIX)].copy()
    if sheet_name in {"Final_Assemblies", "Final_Set_Details"}:
        families = frame.get("assembled_family", pd.Series("", index=frame.index)).fillna("").astype(str)
        return frame[families.str.strip().str.lower().eq(CPLUS_FAMILY)].copy()
    families = frame.get("product_family", pd.Series("", index=frame.index)).fillna("").astype(str)
    return frame[families.str.strip().str.lower().eq(CPLUS_FAMILY)].copy()


def _assembly_id(row: pd.Series) -> str:
    return _first(row, "product_id", "assembled_product_id", "set_id")


def _find_by_id(frame: pd.DataFrame, assembly_id: str, columns: tuple[str, ...]) -> pd.Series | None:
    for column in columns:
        if column not in frame.columns:
            continue
        matches = frame[frame[column].fillna("").astype(str).str.strip().eq(assembly_id)]
        if len(matches) == 1:
            return matches.iloc[0]
    return None


def _find_by_base_grate(frame: pd.DataFrame, base_id: str, grate_article: str) -> pd.Series | None:
    base = frame.get("base_id", frame.get("base_product_id", pd.Series("", index=frame.index)))
    grate = frame.get("grate_article_number", pd.Series("", index=frame.index))
    matches = frame[
        base.fillna("").astype(str).str.strip().eq(base_id)
        & grate.fillna("").astype(str).str.strip().eq(grate_article)
    ]
    if len(matches) == 1:
        return matches.iloc[0]
    return None


def _all_rows_match(rows: list[pd.Series | None], column: str, predicate: Any) -> bool:
    return all(row is not None and predicate(row.get(column)) for row in rows)


def _tile_free(rows: list[pd.Series | None]) -> bool:
    fields = ("product_name", "component_family", "option_family", "source_text_or_reason")
    return all(
        row is not None
        and not any("tile" in _text(row.get(column)).lower() for column in fields)
        for row in rows
    )


def build_cplus_customer_publication_review(
    sheets: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    """Return one diagnostic row per C+ final assembly without mutating input frames."""
    missing = [name for name in REQUIRED_SHEETS if name not in sheets]
    if missing:
        raise ValueError(f"Missing required workbook sheets: {', '.join(missing)}")

    scoped = {name: _cplus_rows(sheets[name], name) for name in REQUIRED_SHEETS}
    final_rows = scoped["Final_Assemblies"]
    review_rows: list[dict[str, Any]] = []

    for _, final_row in final_rows.sort_values("product_id", kind="stable").iterrows():
        assembly_id = _assembly_id(final_row)
        base_id = _first(final_row, "base_id", "base_product_id")
        grate_id = _first(final_row, "grate_id", "grate_component_id", "component_id")
        grate_article = _first(final_row, "grate_article_number")

        product_row = _find_by_id(scoped["Products"], assembly_id, ("product_id",))
        comparison_row = _find_by_id(scoped["Comparison"], assembly_id, ("product_id",))
        detail_row = _find_by_id(
            scoped["Final_Set_Details"], assembly_id,
            ("set_id", "assembled_product_id", "product_id"),
        )
        bom_row = _find_by_base_grate(scoped["BOM_Options"], base_id, grate_article)
        evidence_row = _find_by_base_grate(
            scoped["Cplus_Compatible_Grate_Evidence"], base_id, grate_article
        )
        linked_rows = [product_row, comparison_row, final_row, detail_row, bom_row, evidence_row]
        production_rows = [product_row, comparison_row, final_row, detail_row]
        evidence_rows = [product_row, comparison_row, final_row, bom_row, evidence_row]

        protected = PROTECTED_BASES.get(base_id)
        hydraulic_checks = {
            field: bool(
                protected is not None
                and _all_rows_match(
                    [product_row, comparison_row, final_row, detail_row, evidence_row],
                    field,
                    lambda value, expected=protected[field]: (
                        _text(value) == _text(expected)
                        if field == "outlet_dn"
                        else _numeric_equal(value, expected)
                    ),
                )
            )
            for field in TECHNICAL_FIELDS
        }
        body_articles = {
            token.strip()
            for row in linked_rows
            if row is not None
            for token in _text(row.get("base_article_number")).replace("|", ",").split(",")
            if token.strip()
        }

        checks = {
            "present_in_all_required_sheets": all(row is not None for row in linked_rows),
            "protected_base": base_id in PROTECTED_BASES,
            "approved_grate_article": grate_article in APPROVED_GRATE_ARTICLES,
            "no_tile_article": _tile_free(linked_rows),
            "no_self_reference": bool(base_id and grate_id and base_id != grate_id),
            "body_article_not_used_as_grate": bool(grate_article and grate_article not in body_articles),
            "hydraulic_values_match_protected_base": all(hydraulic_checks.values()),
            "explicit_catalog_matrix_high": _all_rows_match(
                evidence_rows,
                "compatibility_evidence_type",
                lambda value: _text(value) == "explicit_catalog_matrix",
            ) and _all_rows_match(
                evidence_rows,
                "compatibility_confidence",
                lambda value: _text(value).lower() == "high",
            ),
            "article_level_compatibility_found": _all_rows_match(
                evidence_rows,
                "article_level_compatibility_found",
                _truthy,
            ),
            "production_data_quality_status": _all_rows_match(
                production_rows,
                "data_quality_status",
                lambda value: _text(value) == "explicit_source_ready_production_assembly",
            ),
            "ready_for_benchmark": _all_rows_match(
                [product_row, comparison_row, final_row, detail_row, evidence_row],
                "ready_for_benchmark",
                _truthy,
            ),
            "production_ready_for_customer_view": _all_rows_match(
                production_rows,
                "ready_for_customer_view",
                _truthy,
            ),
            "production_customer_view_enabled": _all_rows_match(
                [product_row, comparison_row, final_row],
                "customer_view_enabled",
                _truthy,
            ),
            "diagnostic_evidence_customer_view_disabled": _all_rows_match(
                [evidence_row],
                "ready_for_customer_view",
                _falsey,
            ),
        }
        failures = [name for name, passed in checks.items() if not passed]
        eligible = not failures
        review_rows.append({
            "assembly_id": assembly_id,
            "base_id": base_id,
            "grate_id": grate_id,
            "grate_article_number": grate_article,
            **{f"check_{name}": passed for name, passed in checks.items()},
            "classification": APPROVED if eligible else BLOCKED,
            "publication_gate": APPROVED_GATE if eligible else BLOCKED,
            "blocking_reasons": ",".join(failures),
            "recommended_next_action": (
                RECOMMENDED_NEXT_ACTION if eligible
                else "Resolve every blocking diagnostic before customer publication."
            ),
        })

    return pd.DataFrame(review_rows)


def workbook_diagnostics(
    sheets: Mapping[str, pd.DataFrame], review: pd.DataFrame
) -> dict[str, Any]:
    """Return baseline and non-promotion diagnostics for CLI/tests."""
    final = sheets["Final_Assemblies"]
    families = final.get("assembled_family", pd.Series("", index=final.index)).fillna("").astype(str)
    cplus_products = _cplus_rows(sheets["Products"], "Products")
    cplus_evidence = _cplus_rows(
        sheets["Cplus_Compatible_Grate_Evidence"], "Cplus_Compatible_Grate_Evidence"
    )
    mplus = final[families.eq("showerdrain_mplus")]
    easyflow = final[families.eq("easyflow")]
    details = sheets["Final_Set_Details"]
    detail_families = (
        details.get("assembled_family", pd.Series("", index=details.index))
        .fillna("").astype(str).str.strip().str.lower()
    )
    easyflow_details = details[detail_families.eq("easyflow")]
    easyflow_assemblies_blocked = bool(
        len(easyflow)
        and easyflow.get("data_quality_status", pd.Series("", index=easyflow.index))
        .fillna("").astype(str).str.strip().str.lower().eq("partial").all()
        and easyflow.get(
            "is_complete_technical_data", pd.Series(None, index=easyflow.index)
        ).map(_falsey).all()
        and easyflow.get("missing_technical_fields", pd.Series("", index=easyflow.index))
        .fillna("").astype(str).str.strip().eq(EASYFLOW_MISSING_TECHNICAL_FIELDS).all()
    )
    easyflow_details_blocked = bool(
        len(easyflow_details) == len(easyflow)
        and len(easyflow_details)
        and easyflow_details.get(
            "data_quality_status", pd.Series("", index=easyflow_details.index)
        ).fillna("").astype(str).str.strip().str.lower().eq("partial").all()
        and easyflow_details.get(
            "ready_for_benchmark", pd.Series(None, index=easyflow_details.index)
        ).map(_falsey).all()
        and easyflow_details.get(
            "ready_for_customer_view", pd.Series(None, index=easyflow_details.index)
        ).map(_falsey).all()
    )
    return {
        "Products": len(sheets["Products"]),
        "Comparison": len(sheets["Comparison"]),
        "BOM_Options": len(sheets["BOM_Options"]),
        "Final_Assemblies": len(final),
        "Final_Set_Details": len(sheets["Final_Set_Details"]),
        "Cplus_assemblies": len(cplus_products),
        "Cplus_evidence": len(cplus_evidence),
        "Eplus_assemblies": int(families.eq("showerdrain_eplus").sum()),
        "Bline_assemblies": int(families.eq("showerdrain_b").sum()),
        "Mplus_blocked": bool(
            len(mplus)
            and mplus.get("ready_for_benchmark", pd.Series(False, index=mplus.index)).map(_falsey).all()
            and mplus.get("ready_for_customer_view", pd.Series(False, index=mplus.index)).map(_falsey).all()
        ),
        "Easyflow_blocked": easyflow_assemblies_blocked and easyflow_details_blocked,
        "review_rows": len(review),
        "approved_customer_view_rows": int(
            review.get("classification", pd.Series(dtype=str)).eq(APPROVED).sum()
        ),
        "blocked_rows": int(review.get("classification", pd.Series(dtype=str)).eq(BLOCKED).sum()),
        "customer_flags_approved_enabled": bool(
            len(cplus_products)
            and cplus_products.get(
                "ready_for_customer_view", pd.Series(False, index=cplus_products.index)
            ).map(_truthy).all()
            and cplus_products.get(
                "customer_view_enabled", pd.Series(False, index=cplus_products.index)
            ).map(_truthy).all()
        ),
    }


def load_review_sheets(path: str | Path) -> dict[str, pd.DataFrame]:
    """Load only review inputs from an existing workbook."""
    with pd.ExcelFile(path, engine="openpyxl") as xls:
        missing = [name for name in REQUIRED_SHEETS if name not in xls.sheet_names]
        if missing:
            raise ValueError(f"Missing required workbook sheets: {', '.join(missing)}")
        names = list(REQUIRED_SHEETS) + [
            name for name in OPTIONAL_READINESS_SHEETS if name in xls.sheet_names
        ]
        return {name: pd.read_excel(xls, sheet_name=name) for name in names}


def _print_report(path: Path, review: pd.DataFrame, diagnostics: Mapping[str, Any]) -> None:
    print("ACO ShowerDrain C+ customer-publication review")
    print(f"Workbook: {path}")
    print("Mode: read-only diagnostic; no workbook or customer-view flags were changed")
    print("\nBaseline diagnostics:")
    for name, value in diagnostics.items():
        print(f"- {name}: {value}")
    print("\nAssembly review:")
    for row in review.itertuples(index=False):
        suffix = f" blocking={row.blocking_reasons}" if row.blocking_reasons else ""
        print(
            f"- {row.assembly_id}: {row.classification}; "
            f"publication_gate={row.publication_gate}{suffix}"
        )
    overall = (
        APPROVED
        if len(review) == EXPECTED_CPLUS_ASSEMBLIES and review["classification"].eq(APPROVED).all()
        else BLOCKED
    )
    print(f"\nOVERALL: {overall}")
    print(f"Recommended next action: {RECOMMENDED_NEXT_ACTION}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xlsx", required=True, help="Existing canonical benchmark workbook")
    parser.add_argument(
        "--csv-out",
        help="Optional path for a separate diagnostic CSV; the input XLSX remains unchanged",
    )
    args = parser.parse_args(argv)
    path = Path(args.xlsx)
    try:
        sheets = load_review_sheets(path)
        review = build_cplus_customer_publication_review(sheets)
        diagnostics = workbook_diagnostics(sheets, review)
    except (FileNotFoundError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.csv_out:
        review.to_csv(args.csv_out, index=False)
    _print_report(path, review, diagnostics)
    baseline_ok = all((
        diagnostics["Products"] == 80,
        diagnostics["Comparison"] == 80,
        diagnostics["BOM_Options"] == 251,
        diagnostics["Final_Assemblies"] == 62,
        diagnostics["Final_Set_Details"] == 62,
        diagnostics["Cplus_assemblies"] == EXPECTED_CPLUS_ASSEMBLIES,
        diagnostics["Cplus_evidence"] == EXPECTED_CPLUS_ASSEMBLIES,
        diagnostics["Eplus_assemblies"] == 0,
        diagnostics["Bline_assemblies"] == 0,
        diagnostics["Mplus_blocked"],
        diagnostics["Easyflow_blocked"],
        diagnostics["customer_flags_approved_enabled"],
    ))
    review_ok = (
        len(review) == EXPECTED_CPLUS_ASSEMBLIES
        and review["classification"].eq(APPROVED).all()
        and review["publication_gate"].eq(APPROVED_GATE).all()
    )
    return 0 if baseline_ok and review_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
