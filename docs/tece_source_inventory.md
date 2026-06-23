# TECE Source Inventory

The TECE inventory path is diagnostic-only. It does not add TECE rows to the canonical ACO export tables and it must not mark any TECE item as ready for benchmark or customer view.

## Live source blocker

The live connector remains in place and continues to report the current acquisition blocker from `produktdaten.tece.de`: seed requests can return HTTP 202 / asynchronous placeholder responses, producing zero accepted live candidates. In that state the report keeps:

- `production_promotion_blocked=True`
- `ready_for_benchmark=False`
- `ready_for_customer_view=False`
- `OVERALL: TECE_SOURCE_INVENTORY_BLOCKED_LIVE_SOURCE`

This branch does not replace the TECE connector and does not promote any TECE product to production.

## Source-pack workflow

`tools/report_tece_source_inventory.py` supports an optional local source-pack path:

```bash
python tools/report_tece_source_inventory.py --source-pack tests/fixtures/tece/source_pack
python tools/report_tece_source_inventory.py --source-pack tests/fixtures/tece/source_pack --json
python tools/report_tece_source_inventory.py --source-pack tests/fixtures/tece/source_pack --json --out tests/fixtures/tece/source_pack/inventory_report.json
python tools/report_tece_source_pack_classification.py --source-pack tests/fixtures/tece/source_pack --json --out tests/fixtures/tece/source_pack/classification_report.json
python tools/report_tece_evidence_gap.py --source-pack tests/fixtures/tece/source_pack --json --out tests/fixtures/tece/source_pack/evidence_gap_report.json
```

When saving JSON on Windows CMD, use `--out` instead of shell redirection; the report tools write UTF-8 files and preserve Unicode catalogue text such as `≥`. When a source pack is supplied, the report includes both the live acquisition status and a local read-only ingestion status. Supported local fixture/source file types are HTML, TXT, PDF when local PDF text extraction dependencies are available, JSON, and CSV. The loader extracts manifest-listed source entries only; generated source-pack output files named `inventory_report.json`, `classification_report.json`, or `evidence_gap_report.json` may sit in the pack without being ingested as source rows and are ignored outputs. The loader extracts diagnostic inventory fields only, including article numbers, product family/name, technical fields, evidence text, missing fields, confidence, compatibility evidence status, source filename, page-range metadata, and the same production-blocking flags.

If the source pack contains article candidates and technical fields but no explicit article-level compatibility matrix, the overall status is still incomplete:

- `OVERALL: TECE_SOURCE_PACK_INVENTORY_INCOMPLETE`
- `production_promotion_blocked=True`
- `ready_for_benchmark=False`
- `ready_for_customer_view=False`

The test source pack under `tests/fixtures/tece/source_pack/` is synthetic fixture data only. It is not approved benchmark evidence and must not be used as production TECE evidence. Repeated manifest entries for the same catalogue PDF are supported when each entry declares its own `page_start`, `page_end`, and `page_range_label`; reports count the labels that produce rows and preserve that metadata on each row.

## Evidence still needed

Before TECE can be evaluated for production promotion in a future branch, collect real TECE source files for:

1. Article-level product data.
2. Technical datasheets with dimensions, hydraulic data, outlet data, water seal, and height/installation-height details.
3. Cover/grate compatibility matrix with explicit article-level relationships.
4. Assembly/set matrix if available, including channel body, covers/grates, traps/outlets, feet/accessories, and complete sets.

Until those files are available and reviewed, TECE remains diagnostic-only and production-blocked.
