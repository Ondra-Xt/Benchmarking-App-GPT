from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app_export import AppExportValidationError
from src.canonical_aco_export import export_canonical_aco_workbook, workbook_summary
from src.config import default_config

DEFAULT_TEMPLATE = REPO_ROOT / "data" / "templates" / "benchmark_template.xlsx"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the canonical ACO-only benchmark XLSX and publish it only after validation."
    )
    parser.add_argument("--out", required=True, type=Path, help="Destination benchmark_output.xlsx")
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE)
    args = parser.parse_args(argv)

    try:
        output = export_canonical_aco_workbook(
            args.template,
            args.out,
            default_config(),
        )
    except AppExportValidationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print("OVERALL: FAIL", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        print("OVERALL: FAIL", file=sys.stderr)
        return 1

    counts = workbook_summary(output)
    print(f"Output: {output}")
    for name in (
        "Products",
        "BOM_Options",
        "Final_Assemblies",
        "Cplus_Compatible_Grate_Evidence",
        "Cplus_assembled",
    ):
        print(f"{name}: {counts[name]}")
    print("OVERALL: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
