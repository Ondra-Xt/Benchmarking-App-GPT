# TECE Real Source-Pack Intake

TECE source-pack intake is a **diagnostic-only** workflow for organizing real TECE evidence while live acquisition remains blocked by HTTP 202 asynchronous placeholder responses. It must not add TECE rows to canonical production sheets and must not mark TECE data `ready_for_benchmark` or `ready_for_customer_view`.

## Create a real source-pack folder

1. Create a local folder outside the repository for proprietary/customer files unless the files are approved public fixtures.
2. Add one evidence file per source document or export. Supported extensions are: `.html`, `.htm`, `.txt`, `.pdf`, `.json`, and `.csv`.
3. Add `tece_source_pack_manifest.json` in the folder.
4. Do not include generated canonical production exports (`Products`, `Comparison`, `BOM_Options`, `Final_Assemblies`, or `Final_Set_Details`) as TECE source-pack inputs.
5. The diagnostic report outputs `inventory_report.json`, `classification_report.json`, and `evidence_gap_report.json` may be saved in the source-pack folder for convenience; the validator and ingestion loader treat exactly those filenames as generated outputs, not source inputs. Arbitrary unknown files still fail validation.

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

The validator fails when the manifest is missing, a listed file is missing, an unknown non-generated-output file is present, an unsupported extension is used, required classification is missing, or a source attempts to mark TECE data as production-ready. The only ignored generated output filenames are `inventory_report.json`, `classification_report.json`, and `evidence_gap_report.json`; use a separate `reports/` folder for any other ad-hoc outputs unless they are listed as source evidence in the manifest.

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

## Evidence-gap report

Use the evidence-gap report after the validator, inventory report, and classification report to summarize what is still missing before any future TECE production-promotion review:

```bash
python tools/report_tece_evidence_gap.py --source-pack <path>
python tools/report_tece_evidence_gap.py --source-pack <path> --json
```

The report consumes the existing source-pack manifest, source-pack ingestion output, and classification summary. It is **diagnostic-only**: it does not write canonical export rows, does not change ACO validation counts, does not infer compatibility, and always keeps:

- `production_promotion_blocked: true`
- `ready_for_benchmark: false`
- `ready_for_customer_view: false`

The JSON/plain-text payload includes source-pack counts, article numbers, family and role counts, evidence-scope counts, technical-field coverage, missing-field counts, compatibility/assembly evidence booleans, a `gap_summary`, and prioritized `recommended_next_actions`.

### Gap status interpretation

- `OVERALL: TECE_EVIDENCE_GAP_SYNTHETIC_ONLY` means the pack is the checked-in synthetic fixture set or otherwise contains only synthetic/unapproved fixture evidence. Synthetic fixtures are useful for tests only; they are not production evidence.
- `OVERALL: TECE_EVIDENCE_GAP_COMPATIBILITY_BLOCKED` means at least one non-synthetic/approved source exists, but explicit cover/grate compatibility matrix evidence is still missing. Compatibility must not be inferred from names, families, lengths, or nearby text.
- `OVERALL: TECE_EVIDENCE_GAP_TECHNICAL_DATA_INCOMPLETE` means compatibility evidence is present but required technical data is still incomplete or policy review of conditional values is not complete.

This branch intentionally never returns a production-ready TECE gap status. Even if a future local source pack contains better evidence, the report remains a diagnostic checklist for a later, separately reviewed production-promotion branch.

### Recommended next actions

The report always lists the next actions in this priority order:

1. Replace synthetic fixtures with real TECE public/approved source files.
2. Add article-level TECE channel/body product data.
3. Add article-level cover/grate product data.
4. Add explicit cover/grate compatibility matrix.
5. Add technical datasheets for each production article or complete set.
6. Preserve all conditional technical values with their conditions.
7. Run validator, inventory report, classification report, and gap report again.

## Large catalogue PDFs and page-range manifests

Large TECE catalogue PDFs may contain unrelated WC/module articles alongside drainage sections. To keep source-pack ingestion diagnostic and focused, PDF manifest entries may include optional page-range metadata:

- `page_start` — first catalogue page to extract, as an integer.
- `page_end` — last catalogue page to extract, as an integer.
- `page_range_label` — human-readable label used in reports and JSON counts.

Page ranges are supported only for PDF sources. If either `page_start` or `page_end` is present, both must be present and `page_start` must be less than or equal to `page_end`. The same PDF can be listed multiple times in the manifest with different page ranges; each manifest entry is processed as an independent source unit rather than being deduplicated by filename. During ingestion, only the selected PDF pages are extracted for each entry, and each extracted diagnostic row records `source_page_start`, `source_page_end`, and `page_range_label`. This does not change production gating: TECE remains diagnostic-only, production promotion remains blocked, and compatibility must not be inferred.

Example:

```json
{
  "source_file": "Sortimentsliste_TECE_DE_2026_web.pdf",
  "source_type": "pdf",
  "source_origin": "manual_download",
  "source_url": "https://www.tece.com/.../Sortimentsliste_TECE_DE_2026_web.pdf",
  "source_date": "2026-06-23",
  "document_title": "TECE Sortimentsliste 2026 Deutschland",
  "product_family_hint": "TECEdrainline",
  "evidence_scope": "article_data",
  "page_start": 271,
  "page_end": 294,
  "page_range_label": "TECEdrainline drainage section",
  "approved_for_benchmark_evidence": false,
  "notes": "Real TECE catalogue source candidate; diagnostic-only."
}
```

Recommended drainage ranges for `Sortimentsliste_TECE_DE_2026_web.pdf` intake are:

| Product family | Catalogue pages | Suggested `page_range_label` |
| --- | ---: | --- |
| TECEdrainway | 245-256 | `TECEdrainway drainage section` |
| TECEdrainprofile | 257-270 | `TECEdrainprofile drainage section` |
| TECEdrainline | 271-294 | `TECEdrainline drainage section` |
| TECEdrainpoint S | 295-326 | `TECEdrainpoint S drainage section` |

The validator and reports include `page_range_label_counts` when labels are present and keep existing behavior for source packs without page-range metadata. Inventory, classification, and gap report JSON keeps the source filename and page-range metadata on each row so repeated catalogue entries remain traceable.
