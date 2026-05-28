import sys
from dataclasses import dataclass

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from src.excel_export import _scoring_field_coverage

TECH_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)
EASYFLOW_ASSEMBLED_PREFIX = "aco-assembled-easyflow-"


@dataclass
class EasyflowRowDiagnostic:
    assembled_id: str
    base_id: str
    component_id: str
    parse_ok: bool
    assembled_fields: dict[str, str]
    base_source: str
    base_fields: dict[str, str]
    component_source: str
    component_fields: dict[str, str]
    verdict: str


def _norm(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def _get_fields(row: pd.Series | None) -> dict[str, str]:
    if row is None:
        return {k: "" for k in TECH_FIELDS}
    out: dict[str, str] = {}
    for k in TECH_FIELDS:
        v = row.get(k, "")
        out[k] = "" if pd.isna(v) else str(v)
    return out


def _is_missing(value: str) -> bool:
    return str(value).strip() == ""


def parse_assembled_id(product_id: str) -> tuple[str, str, bool]:
    if not product_id.startswith(EASYFLOW_ASSEMBLED_PREFIX):
        return "", "", False
    tail = product_id[len("aco-assembled-"):]
    if "__" not in tail:
        return "", "", False
    base_slug, comp_slug = tail.split("__", 1)
    if not base_slug or not comp_slug:
        return "", "", False
    return f"aco-{base_slug}", f"aco-{comp_slug}", True


def _locate_row(product_id: str, products: pd.DataFrame, comparison: pd.DataFrame, excluded: pd.DataFrame, registry: pd.DataFrame) -> tuple[str, pd.Series | None]:
    for name, df in (("Products", products), ("Comparison", comparison), ("Components", excluded), ("Candidates_All", registry)):
        match = df[_norm(df, "product_id") == product_id]
        if not match.empty:
            return name, match.iloc[0]
    return "missing", None


def diagnose_easyflow_rows(products: pd.DataFrame, comparison: pd.DataFrame, excluded: pd.DataFrame, registry: pd.DataFrame) -> list[EasyflowRowDiagnostic]:
    assembled = products[_norm(products, "product_id").str.startswith(EASYFLOW_ASSEMBLED_PREFIX)].copy()
    reports: list[EasyflowRowDiagnostic] = []
    for _, row in assembled.iterrows():
        assembled_id = str(row.get("product_id") or "")
        base_id, component_id, parse_ok = parse_assembled_id(assembled_id)
        assembled_fields = _get_fields(row)

        if not parse_ok:
            reports.append(EasyflowRowDiagnostic(assembled_id, "", "", False, assembled_fields, "missing", _get_fields(None), "missing", _get_fields(None), "assembled product_id parsing failed"))
            continue

        base_source, base_row = _locate_row(base_id, products, comparison, excluded, registry)
        component_source, component_row = _locate_row(component_id, products, comparison, excluded, registry)
        base_fields = _get_fields(base_row)
        component_fields = _get_fields(component_row)

        if base_row is None:
            verdict = "base_id does not exist in final universe"
        elif component_row is None:
            verdict = "component_id mismatch"
        elif all(_is_missing(v) for v in base_fields.values()):
            verdict = "base row also missing fields"
        elif all(_is_missing(v) for v in assembled_fields.values()) and any(not _is_missing(v) for v in base_fields.values()):
            verdict = "base row has fields but inheritance did not copy them"
        else:
            verdict = "no missing-field inheritance issue detected"

        reports.append(EasyflowRowDiagnostic(assembled_id, base_id, component_id, True, assembled_fields, base_source, base_fields, component_source, component_fields, verdict))
    return reports


def print_report(reports: list[EasyflowRowDiagnostic], counts: dict[str, int]) -> None:
    print("Easyflow assembled technical-field diagnostic")
    for k, v in counts.items():
        print(f"- {k}: {v}")
    print("\nPer-row diagnostics:")
    for rep in reports:
        print(f"\nassembled_id: {rep.assembled_id}")
        print(f"- parse_ok: {rep.parse_ok}")
        print(f"- base_id: {rep.base_id}")
        print(f"- component_id: {rep.component_id}")
        print(f"- base_source: {rep.base_source}")
        print(f"- component_source: {rep.component_source}")
        print("- assembled fields:")
        for f in TECH_FIELDS:
            print(f"  - {f}: {rep.assembled_fields[f]}")
        print("- base fields:")
        for f in TECH_FIELDS:
            print(f"  - {f}: {rep.base_fields[f]}")
        print("- component fields:")
        for f in TECH_FIELDS:
            print(f"  - {f}: {rep.component_fields[f]}")
        print(f"- verdict: {rep.verdict}")


def main() -> int:
    registry_rows, _ = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, excluded, _evidence, bom = pipeline.run_update(registry, default_config())
    coverage = _scoring_field_coverage(products, comparison)

    reports = diagnose_easyflow_rows(products, comparison, excluded, registry)

    counts = {
        "Products": len(products),
        "Comparison": len(comparison),
        "Scoring_Field_Coverage": len(coverage),
        "Candidates_All": len(registry),
        "Components": len(excluded),
        "BOM_Options": len(bom),
        "total assembled rows": int(_norm(products, "product_id").str.startswith("aco-assembled-").sum()),
        "easyflow assembled": len(reports),
    }
    print_report(reports, counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
