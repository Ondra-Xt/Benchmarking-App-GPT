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

- Real, approved TECE article-level evidence.
- Complete technical datasheets for required benchmark fields.
- Explicit cover/grate compatibility matrix evidence where assemblies are evaluated.
- Explicit assembly matrix evidence where finished sets or component assemblies are evaluated.
- Manual review confirming no compatibility has been inferred.
- Separate changes to production promotion logic and expected counts in a future branch.

Until then, TECE remains diagnostic-only and production-blocked.
