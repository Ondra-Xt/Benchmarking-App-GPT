# TECE Real Source-Pack Intake

TECE source-pack intake is a **diagnostic-only** workflow for organizing real TECE evidence while live acquisition remains blocked by HTTP 202 asynchronous placeholder responses. It must not add TECE rows to canonical production sheets and must not mark TECE data `ready_for_benchmark` or `ready_for_customer_view`.

## Create a real source-pack folder

1. Create a local folder outside the repository for proprietary/customer files unless the files are approved public fixtures.
2. Add one evidence file per source document or export. Supported extensions are: `.html`, `.htm`, `.txt`, `.pdf`, `.json`, and `.csv`.
3. Add `tece_source_pack_manifest.json` in the folder.
4. Do not include generated canonical production exports (`Products`, `Comparison`, `BOM_Options`, `Final_Assemblies`, or `Final_Set_Details`) as TECE source-pack inputs.

The checked-in folder `tests/fixtures/tece/source_pack/` is synthetic test-only data. It is not approved benchmark evidence.

## Manifest format

`tece_source_pack_manifest.json` may be a JSON object with a `sources` list. Each source entry should use this shape:

```json
{
  "sources": [
    {
      "source_file": "example_tece_datasheet.pdf",
      "source_type": "pdf",
      "source_origin": "public_url",
      "source_url": "https://example.com/source.pdf",
      "source_date": "2026-06-22",
      "document_title": "TECE document title",
      "product_family_hint": "TECEdrainline",
      "evidence_scope": "technical_datasheet",
      "approved_for_benchmark_evidence": false,
      "notes": "Diagnostic intake only; not production evidence."
    }
  ]
}
```

`approved_for_benchmark_evidence` must remain `false` during intake. Omitted values are treated as false by the inventory reporter, but the strict validator requires the field to be explicit so review status is transparent.

## Accepted source origins

Use one of these `source_origin` values:

- `public_url` — directly reachable public TECE or distributor URL.
- `manual_download` — manually downloaded source document.
- `supplier_export` — supplier-provided export file.
- `unknown` — origin is not yet established.

## Evidence scopes

Use one of these `evidence_scope` values for every listed source:

- `article_data` — article number, product name, or family evidence.
- `technical_datasheet` — dimensions, flow rate, outlet, water seal, installation height, or similar technical fields.
- `cover_grate_matrix` — explicit cover/grate compatibility matrix evidence.
- `assembly_matrix` — explicit assembly/component matrix evidence.
- `unknown` — source requires manual classification before use.

Compatibility must not be inferred. If a file contains cover/grate or assembly compatibility evidence, it must be identified explicitly with `cover_grate_matrix` or `assembly_matrix`.

## Diagnostic classification

The source-pack inventory adds a **diagnostic-only** classification layer for each extracted row. The classifier is intentionally conservative and records:

- `tece_article_role_candidate` — one of `channel_body`, `cover_or_grate`, `drain_body`, `complete_set`, `technical_datasheet_only`, `compatibility_matrix`, `assembly_matrix`, or `unknown`.
- `tece_family_candidate` — one of `TECEdrainline`, `TECEdrainprofile`, `TECEdrainpoint`, or `unknown`.
- `classification_confidence` — `low`, `medium`, or `high`, based only on explicit source-pack text and manifest hints.
- `classification_reason` — the keyword or manifest scope rule that produced the candidate classification.
- `production_blocking_reason` — diagnostic blockers such as `missing_article_level_compatibility_matrix`, `missing_counterpart_article`, `missing_technical_fields`, or `synthetic_test_fixture_only`.

Classification is **not compatibility proof**. For example, a row with `evidence_scope: cover_grate_matrix` is classified as `compatibility_matrix`, but that does not make any TECE article production-ready, does not infer body/channel-to-cover/grate compatibility, and does not set `ready_for_benchmark` or `ready_for_customer_view`. The checked-in synthetic fixtures always remain blocked with `synthetic_test_fixture_only`.

Generate the focused classification report with:

```bash
python tools/report_tece_source_pack_classification.py --source-pack <path>
python tools/report_tece_source_pack_classification.py --source-pack <path> --json
```

The main inventory report also includes `source_pack_classification_summary` with role, family, confidence, and production-blocking reason counts.

## Validation and inventory commands

Validate a source pack:

```bash
python tools/validate_tece_source_pack.py --source-pack <path>
```

Generate a diagnostic inventory report:

```bash
python tools/report_tece_source_inventory.py --source-pack <path> --json
```

The validator fails when the manifest is missing, a listed file is missing, an unknown file is present, an unsupported extension is used, required classification is missing, or a source attempts to mark TECE data as production-ready.

## Requirements before any future production promotion

Production promotion remains blocked in this branch. A future promotion review would require, at minimum:

- Article-level body/channel article data.
- Article-level cover/grate article data.
- An explicit body/channel-to-cover/grate compatibility matrix.
- Technical datasheet evidence per production article or complete set.
- Conditional technical values preserved with their stated conditions.
- Manual review confirming no compatibility has been inferred.
- Separate changes to production promotion logic and expected counts in a future branch.

Until then, TECE remains diagnostic-only and production-blocked.
