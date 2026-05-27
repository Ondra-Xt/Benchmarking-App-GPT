import argparse
import sys
from pathlib import Path
from typing import List

import pandas as pd

from tools.validate_latest_xlsx_export import DEFAULT_PATTERNS, pick_xlsx_file

REQUIRED_SHEETS = ["Products", "Comparison", "Components", "BOM_Options", "Scoring_Field_Coverage"]
TARGET_FAMILY_PREFIXES = [
    ("ShowerDrain S+", "aco-assembled-showerdrain-splus"),
    ("ShowerDrain C", "aco-assembled-showerdrain-c"),
    ("ShowerDrain M+", "aco-assembled-showerdrain-mplus"),
    ("ShowerDrain E+", "aco-assembled-showerdrain-eplus"),
    ("ShowerDrain B", "aco-assembled-showerdrain-b"),
    ("ShowerDrain C+", "aco-assembled-showerdrain-cplus"),
    ("Easyflow", "aco-assembled-easyflow-"),
    ("Easyflow+", "aco-assembled-easyflowplus-"),
]


def _norm_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def _aco_mask(df: pd.DataFrame) -> pd.Series:
    cols = [c for c in ["manufacturer", "product_id", "component_id", "target_product_id", "name", "family"] if c in df.columns]
    if not cols:
        return pd.Series([False] * len(df), index=df.index)
    blob = pd.Series([""] * len(df), index=df.index, dtype="string")
    for col in cols:
        blob = blob + " " + _norm_series(df, col).str.lower()
    return blob.str.contains("aco")


def _group_counts(df: pd.DataFrame, col: str) -> list[str]:
    if col not in df.columns:
        return [f"  - {col}: column missing"]
    vals = _norm_series(df, col).replace("", "<missing>")
    counts = vals.value_counts().sort_index()
    return [f"  - {col}={idx}: {int(val)}" for idx, val in counts.items()]


def _load_required_sheets(xlsx_path: Path) -> dict[str, pd.DataFrame]:
    try:
        workbook = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as exc:
        raise RuntimeError(f"Could not read workbook: {exc}") from exc

    missing = [s for s in REQUIRED_SHEETS if s not in workbook.sheet_names]
    if missing:
        raise ValueError(f"Required sheets missing: {', '.join(missing)}")

    return {sheet: pd.read_excel(xlsx_path, sheet_name=sheet, engine="openpyxl") for sheet in REQUIRED_SHEETS}


def generate_report(xlsx_path: Path) -> tuple[int, str]:
    lines: list[str] = []
    warnings: list[str] = []

    if not xlsx_path.exists() or not xlsx_path.is_file():
        return 2, f"ERROR: XLSX file does not exist: {xlsx_path}"

    try:
        sheets = _load_required_sheets(xlsx_path)
    except (RuntimeError, ValueError) as exc:
        return 2, f"ERROR: {exc}"

    products = sheets["Products"]
    comparison = sheets["Comparison"]
    components = sheets["Components"]
    bom = sheets["BOM_Options"]
    coverage = sheets["Scoring_Field_Coverage"]

    aco_products = products[_aco_mask(products)]
    aco_comparison = comparison[_aco_mask(comparison)]
    aco_components = components[_aco_mask(components)]
    aco_bom = bom[_aco_mask(bom)]

    if any(len(df) == 0 for df in [aco_products, aco_comparison, aco_components, aco_bom]):
        return 3, "ERROR: No ACO rows found in one or more required sheets (Products, Comparison, Components, BOM_Options)."

    lines.append("1. Selected workbook")
    lines.append(f"  - path: {xlsx_path}")

    lines.append("2. ACO Products summary")
    lines.append(f"  - aco_rows: {len(aco_products)}")
    lines.extend(_group_counts(aco_products, "family"))
    lines.extend(_group_counts(aco_products, "system_role"))
    lines.extend(_group_counts(aco_products, "assembled_from_bom"))
    if "assembled_from_bom" in aco_products.columns and "product_id" in aco_products.columns:
        assembled = aco_products[_norm_series(aco_products, "assembled_from_bom").str.lower().isin(["true", "1", "yes"])]["product_id"].dropna().astype(str).tolist()
        lines.append(f"  - assembled_product_ids: {', '.join(sorted(assembled)) if assembled else '<none>'}")
    else:
        lines.append("  - assembled_product_ids: <unavailable>")

    lines.append("3. ACO Components summary")
    lines.append(f"  - aco_rows: {len(aco_components)}")
    lines.extend(_group_counts(aco_components, "family"))
    lines.extend(_group_counts(aco_components, "component_role"))
    lines.extend(_group_counts(aco_components, "option_type"))
    likely_products = pd.Series([False] * len(aco_components), index=aco_components.index)
    if "system_role" in aco_components.columns:
        likely_products = likely_products | _norm_series(aco_components, "system_role").str.contains("drain_unit|product", case=False, regex=True)
    if "product_id" in aco_components.columns:
        likely_products = likely_products | _norm_series(aco_components, "product_id").str.contains("showerdrain|easyflow", case=False, regex=True)
    heuristic_count = int(likely_products.sum())
    lines.append(f"  - heuristic_product_like_component_rows: {heuristic_count} (non-failing heuristic)")
    if heuristic_count > 0:
        warnings.append("Heuristic only: some component rows look product-like; review if unexpected.")

    lines.append("4. ACO BOM_Options summary")
    lines.append(f"  - aco_rows: {len(aco_bom)}")
    lines.extend(_group_counts(aco_bom, "option_type"))
    option_counts = _norm_series(aco_bom, "option_type").str.lower().value_counts()
    lines.append(f"  - optional_accessory_count: {int(option_counts.get('optional_accessory', 0))}")
    lines.append(f"  - compatible_grate_count: {int(option_counts.get('compatible_grate', 0))}")

    left = _norm_series(aco_bom, "product_id")
    right = _norm_series(aco_bom, "component_id")
    self_refs = (left != "") & (right != "") & (left == right)
    component_roles = _norm_series(components, "system_role").str.lower()
    component_ids = _norm_series(components, "product_id")
    grate_ids = set(component_ids[component_roles.eq("grate")])
    grate_to_grate = _norm_series(aco_bom, "option_type").str.lower().eq("compatible_grate") & right.isin(grate_ids) & left.isin(grate_ids)
    missing_target = left.eq("") | right.eq("")
    lines.append(f"  - self_reference_rows: {int(self_refs.sum())}")
    lines.append(f"  - grate_to_grate_links: {int(grate_to_grate.sum())}")
    lines.append(f"  - rows_missing_target_or_component: {int(missing_target.sum())}")

    lines.append("5. ACO assembled product families")
    assembled_product_ids = _norm_series(aco_products, "product_id").str.lower()
    for family, prefix in TARGET_FAMILY_PREFIXES:
        lines.append(f"  - {family}: {int(assembled_product_ids.str.startswith(prefix).sum())}")

    lines.append("6. ACO Scoring_Field_Coverage completeness")
    aco_coverage = coverage[_aco_mask(coverage)]
    if len(aco_coverage) == 0:
        warnings.append("No ACO rows found in Scoring_Field_Coverage.")
        lines.append("  - no ACO rows available")
    else:
        skip_cols = {"manufacturer", "product_id", "family", "name", "system_role"}
        data_cols = [c for c in aco_coverage.columns if c not in skip_cols]
        for col in sorted(data_cols):
            s = aco_coverage[col]
            present = int((~s.isna() & (s.astype(str).str.strip() != "")).sum())
            missing = int(len(s) - present)
            pct = (missing / len(s) * 100.0) if len(s) else 0.0
            lines.append(f"  - field={col} present={present} missing={missing} missing_pct={pct:.1f}%")
            if present == 0:
                warnings.append(f"Coverage field systematically missing for ACO rows: {col}")

        if "family" in aco_coverage.columns and data_cols:
            lines.append("  - missing_by_family:")
            fam = _norm_series(aco_coverage, "family").replace("", "<missing>")
            for family_name in sorted(fam.unique()):
                subset = aco_coverage[fam == family_name]
                miss_bits = []
                for col in sorted(data_cols):
                    ps = int((~subset[col].isna() & (subset[col].astype(str).str.strip() != "")).sum())
                    miss_bits.append(f"{col}:{len(subset)-ps}")
                lines.append(f"    - {family_name}: {', '.join(miss_bits)}")

    lines.append("7. Warnings / observations")
    if warnings:
        for w in warnings:
            lines.append(f"  - {w}")
    else:
        lines.append("  - none")

    lines.append("8. Overall report status")
    lines.append("  - PASS")
    return 0, "\n".join(lines)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate ACO-focused QA report from benchmark XLSX export.")
    parser.add_argument("--xlsx", help="Path to workbook")
    parser.add_argument("--dir", dest="directory", help="Directory containing xlsx exports")
    args = parser.parse_args(argv)

    if bool(args.xlsx) == bool(args.directory):
        print("ERROR: Provide exactly one of --xlsx or --dir")
        return 2

    if args.directory:
        try:
            selected = pick_xlsx_file(Path(args.directory), DEFAULT_PATTERNS, include_any_xlsx=False, verbose=False)
        except FileNotFoundError as exc:
            print(f"ERROR: {exc}")
            return 2
        print(f"Selected XLSX: {selected}")
        code, report = generate_report(selected)
    else:
        code, report = generate_report(Path(args.xlsx))

    print(report)
    return code


if __name__ == "__main__":
    sys.exit(main())
