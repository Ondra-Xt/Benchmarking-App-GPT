import sys
from collections import defaultdict

import pandas as pd

from src import pipeline
from src.config import default_config
from src.connectors import aco
from tools.diagnose_aco_assembly_readiness import compute_readiness

ASSEMBLED_PREFIX = "aco-assembled-"
TECH_FIELDS = (
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
)


def _norm(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(["" for _ in range(len(df))], index=df.index, dtype="string")
    return df[col].fillna("").astype(str)


def _assembled_family(product_id: str) -> str:
    base = product_id[len(ASSEMBLED_PREFIX):]
    if base.startswith("showerdrain-splus-"):
        return "showerdrain_splus"
    if base.startswith("showerdrain-c-"):
        return "showerdrain_c"
    if base.startswith("easyflowplus-"):
        return "easyflowplus"
    if base.startswith("easyflow-"):
        return "easyflow"
    return base.split("-", 1)[0] if "-" in base else base


def _missing_fields(row: pd.Series) -> list[str]:
    missing = []
    for field in TECH_FIELDS:
        val = row.get(field, "")
        if pd.isna(val) or str(val).strip() == "":
            missing.append(field)
    return missing


def summarize_final_assemblies(products: pd.DataFrame) -> dict:
    pids = _norm(products, "product_id")
    assembled = products[pids.str.startswith(ASSEMBLED_PREFIX)].copy()
    assembled_ids = _norm(assembled, "product_id")

    ids_by_family = defaultdict(list)
    rows_by_family = defaultdict(list)
    for idx, pid in assembled_ids.items():
        fam = _assembled_family(pid)
        ids_by_family[fam].append(pid)
        rows_by_family[fam].append(assembled.loc[idx])

    for fam in ids_by_family:
        ids_by_family[fam] = sorted(ids_by_family[fam])

    missing_by_id = {}
    for idx, row in assembled.iterrows():
        pid = str(row.get("product_id") or "")
        missing = _missing_fields(row)
        if missing:
            missing_by_id[pid] = missing

    return {
        "total": int(len(assembled)),
        "count_by_family": {fam: len(ids) for fam, ids in sorted(ids_by_family.items())},
        "ids_by_family": dict(sorted(ids_by_family.items())),
        "rows_by_family": dict(sorted(rows_by_family.items())),
        "missing_by_id": dict(sorted(missing_by_id.items())),
    }


def print_report(summary: dict, blocked_lines: list[str]) -> None:
    print("Final assembled products report")
    print(f"total assembled rows: {summary['total']}")

    print("\ncount by family:")
    for family, count in summary["count_by_family"].items():
        print(f"- {family}: {count}")

    print("\nproduct IDs by family:")
    for family, ids in summary["ids_by_family"].items():
        print(f"- {family} ({len(ids)}):")
        for pid in ids:
            print(f"  - {pid}")

    print("\ntechnical fields per assembled row:")
    for family, rows in summary["rows_by_family"].items():
        print(f"- {family}:")
        for row in rows:
            pid = row.get("product_id", "")
            fields = ", ".join([f"{field}={row.get(field, '')}" for field in TECH_FIELDS])
            print(f"  - {pid}: {fields}")

    print("\nmissing technical-field warnings:")
    if not summary["missing_by_id"]:
        print("- none")
    else:
        for pid, fields in summary["missing_by_id"].items():
            print(f"- {pid}: missing {', '.join(fields)}")

    if blocked_lines:
        print("\nblocked/non-assembled families from readiness:")
        for line in blocked_lines:
            print(f"- {line}")


def main() -> int:
    registry_rows, _ = aco.discover_candidates(target_length_mm=1200, tolerance_mm=100)
    registry = pd.DataFrame(registry_rows)
    products, comparison, excluded, _evidence, bom = pipeline.run_update(registry, default_config())

    summary = summarize_final_assemblies(products)
    readiness = compute_readiness(products, comparison, excluded, bom)
    blocked_lines = []
    for fam in ("showerdrain_cplus", "showerdrain_mplus", "showerdrain_eplus"):
        fr = readiness.families[fam]
        if fr.status == "BLOCKED":
            reason = fr.reasons[0] if fr.reasons else "unknown reason"
            blocked_lines.append(f"{fam}: BLOCKED ({reason})")

    print_report(summary, blocked_lines)
    return 0


if __name__ == "__main__":
    sys.exit(main())
