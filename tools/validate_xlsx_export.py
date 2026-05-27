import argparse
import sys
from dataclasses import dataclass
from typing import List, Tuple

import pandas as pd


# Configurable baseline expectations
EXPECTED_SHEET_COUNTS = {
    "Products": 46,
    "Comparison": 46,
    "Scoring_Field_Coverage": 46,
    "Candidates_All": 118,
    "Components": 100,
    "BOM_Options": 221,
}
COMPONENTS_MIN_ROWS = 1
EXPECTED_BOM_OPTION_TYPE_COUNTS = {
    "optional_accessory": 144,
    "compatible_grate": 53,
}
EXPECTED_ASSEMBLED_PREFIX_COUNTS = {
    "aco-assembled-showerdrain-splus": 16,
    "aco-assembled-showerdrain-c": 4,
    "aco-assembled-showerdrain-mplus": 0,
    "aco-assembled-showerdrain-eplus": 0,
    "aco-assembled-showerdrain-b": 0,
    "aco-assembled-showerdrain-cplus": 0,
}
FORBIDDEN_SYSTEM_ROLES = {"grate", "accessory", "optional_accessory"}
REQUIRED_SHEETS = [
    "Products",
    "Comparison",
    "Scoring_Field_Coverage",
    "Candidates_All",
    "Components",
    "BOM_Options",
]
OPTIONAL_SHEETS = ["Evidence"]
CPLUS_EXPECTED = {
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
COMPATIBLE_GRATE_META_REQUIRED_SNIPPETS = [
    "compatibility_confidence=implicit_family_level",
    "explicit_article_matrix=false",
    "source_limitation=",
    "no explicit article-to-article matrix found",
]


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def _norm_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype=str)
    return df[col].fillna("").astype(str).str.strip()


def _compatible_grate_meta_valid(meta: str) -> bool:
    text = str(meta or "").lower()
    return all(snippet in text for snippet in COMPATIBLE_GRATE_META_REQUIRED_SNIPPETS)


def validate_xlsx(path: str) -> Tuple[bool, List[CheckResult]]:
    results: List[CheckResult] = []
    xls = pd.ExcelFile(path, engine="openpyxl")

    missing = [s for s in REQUIRED_SHEETS if s not in xls.sheet_names]
    results.append(CheckResult("required_sheets", not missing, f"missing={missing} expected={REQUIRED_SHEETS}"))
    if missing:
        return False, results

    for s in OPTIONAL_SHEETS:
        results.append(CheckResult(f"optional_sheet:{s}", True, f"present={s in xls.sheet_names}"))

    sheets = {name: pd.read_excel(xls, sheet_name=name) for name in REQUIRED_SHEETS}
    products, comparison, coverage, components, candidates, bom = (
        sheets["Products"],
        sheets["Comparison"],
        sheets["Scoring_Field_Coverage"],
        sheets["Components"],
        sheets["Candidates_All"],
        sheets["BOM_Options"],
    )

    for name, expected in EXPECTED_SHEET_COUNTS.items():
        actual = len(sheets[name])
        results.append(CheckResult(f"row_count:{name}", actual == expected, f"actual={actual} expected={expected}"))

    comp_ok = len(components) >= COMPONENTS_MIN_ROWS
    results.append(CheckResult("components_min_rows", comp_ok, f"actual={len(components)} expected>={COMPONENTS_MIN_ROWS}"))

    cmp_product_ids = _norm_series(comparison, "product_id").str.lower()
    for prefix, expected in EXPECTED_ASSEMBLED_PREFIX_COUNTS.items():
        actual = int(cmp_product_ids.str.startswith(prefix).sum())
        results.append(CheckResult(f"assembled_count:{prefix}", actual == expected, f"actual={actual} expected={expected}"))

    for sheet_name, df in [("Products", products), ("Comparison", comparison)]:
        roles = _norm_series(df, "system_role").str.lower()
        bad = int(roles.isin(FORBIDDEN_SYSTEM_ROLES).sum())
        results.append(CheckResult(f"forbidden_roles:{sheet_name}", bad == 0, f"actual={bad} expected=0"))

    option_types = _norm_series(bom, "option_type").str.lower()
    for option_type, expected in EXPECTED_BOM_OPTION_TYPE_COUNTS.items():
        actual = int((option_types == option_type).sum())
        results.append(CheckResult(f"bom_option_type_count:{option_type}", actual == expected, f"actual={actual} expected={expected}"))

    bom_pid = _norm_series(bom, "product_id")
    bom_cid = _norm_series(bom, "component_id")
    self_refs = int((bom_pid == bom_cid).sum())
    results.append(CheckResult("bom_self_reference", self_refs == 0, f"actual={self_refs} expected=0"))

    comp_roles = _norm_series(components, "system_role").str.lower()
    comp_role_map = {_norm_series(components, "product_id").iloc[i]: comp_roles.iloc[i] for i in range(len(components))}
    grate_ids = {k for k, v in comp_role_map.items() if v == "grate"}
    grate_to_grate = int(((option_types == "compatible_grate") & bom_cid.isin(grate_ids) & bom_pid.isin(grate_ids)).sum())
    results.append(CheckResult("grate_to_grate_links", grate_to_grate == 0, f"actual={grate_to_grate} expected=0"))

    nav_labels = int(comp_roles.eq("navigation-label").sum())
    results.append(CheckResult("navigation_label_components", nav_labels == 0, f"actual={nav_labels} expected=0"))

    universe = set(_norm_series(products, "product_id")).union(set(_norm_series(components, "product_id")))
    aco_mask = _norm_series(bom, "manufacturer").str.lower().eq("aco")
    dangling = int((~bom_cid.isin(universe) & aco_mask).sum())
    results.append(CheckResult("aco_dangling_component_id", dangling == 0, f"actual={dangling} expected=0"))

    compat = bom[option_types == "compatible_grate"].copy()
    bad_meta = 0
    for _, row in compat.iterrows():
        if not _compatible_grate_meta_valid(row.get("option_meta", "")):
            bad_meta += 1
    results.append(CheckResult("compatible_grate_metadata", bad_meta == 0, f"actual={bad_meta} expected=0"))

    for df_name, df in [("Products", products), ("Comparison", comparison)]:
        ids = set(_norm_series(df, "product_id"))
        missing_ids = [pid for pid in CPLUS_EXPECTED if pid not in ids]
        results.append(CheckResult(f"cplus_presence:{df_name}", not missing_ids, f"missing={missing_ids}"))

    product_indexed = products.set_index("product_id") if "product_id" in products.columns else pd.DataFrame()
    for pid, expected_fields in CPLUS_EXPECTED.items():
        if pid not in product_indexed.index:
            continue
        row = product_indexed.loc[pid]
        for field, expected in expected_fields.items():
            actual = row.get(field)
            passed = str(actual) == str(expected) if isinstance(expected, str) else float(actual) == float(expected)
            results.append(CheckResult(f"cplus_value:{pid}:{field}", passed, f"actual={actual} expected={expected}"))

    all_passed = all(r.passed for r in results)
    return all_passed, results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate ACO benchmark XLSX export baseline.")
    parser.add_argument("xlsx_path", help="Path to benchmark export XLSX")
    args = parser.parse_args(argv)

    passed, results = validate_xlsx(args.xlsx_path)

    print("XLSX validation report")
    print("=" * 80)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.name}: {r.detail}")
    print("=" * 80)
    print("OVERALL: PASS" if passed else "OVERALL: FAIL")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
