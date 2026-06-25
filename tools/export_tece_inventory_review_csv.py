from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import load_source_pack

COLUMNS = [
    "article_number", "product_family", "tece_family_candidate", "article_role", "tece_article_role_candidate",
    "product_name", "title", "description", "nominal_length_mm", "width_mm", "finish_or_color", "flow_rate_lps",
    "conditional_technical_values", "outlet_dn", "water_seal_mm", "installation_height_mm", "height_adj_min_mm",
    "height_adj_max_mm", "page_range_label", "source_file", "source_page_start", "source_page_end",
    "catalogue_page_label", "extraction_confidence", "classification_confidence", "classification_reason",
    "production_blocking_reason", "production_promotion_blocked", "ready_for_benchmark", "ready_for_customer_view",
]


def _cell(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def export_inventory_csv(source_pack: str | Path, out: str | Path) -> int:
    report = load_source_pack(source_pack)
    with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        for row in report.rows:
            payload = asdict(row)
            writer.writerow({
                "article_number": row.article_number,
                "product_family": row.product_family,
                "tece_family_candidate": row.tece_family_candidate,
                "article_role": row.tece_article_role_candidate,
                "tece_article_role_candidate": row.tece_article_role_candidate,
                "product_name": row.product_name,
                "title": row.product_name,
                "description": row.evidence_text,
                "nominal_length_mm": row.nominal_length_mm,
                "width_mm": row.width_mm,
                "finish_or_color": row.finish_or_color,
                "flow_rate_lps": row.flow_rate_lps,
                "conditional_technical_values": _cell(row.conditional_technical_values or []),
                "outlet_dn": row.outlet_dn,
                "water_seal_mm": row.water_seal_mm,
                "installation_height_mm": row.installation_height_mm,
                "height_adj_min_mm": row.height_adj_min_mm,
                "height_adj_max_mm": row.height_adj_max_mm,
                "page_range_label": row.page_range_label,
                "source_file": row.source_file,
                "source_page_start": row.source_page_start,
                "source_page_end": row.source_page_end,
                "catalogue_page_label": row.catalogue_page_label,
                "extraction_confidence": payload.get("confidence"),
                "classification_confidence": row.classification_confidence,
                "classification_reason": row.classification_reason,
                "production_blocking_reason": row.production_blocking_reason,
                "production_promotion_blocked": row.production_promotion_blocked,
                "ready_for_benchmark": row.ready_for_benchmark,
                "ready_for_customer_view": row.ready_for_customer_view,
            })
    return len(report.rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export full diagnostic-only TECE source-pack inventory CSV.")
    parser.add_argument("--source-pack", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    export_inventory_csv(args.source_pack, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
