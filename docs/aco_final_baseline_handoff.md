# ACO final baseline audit and manufacturer handoff

## Purpose and scope

This document freezes the final reviewed ACO baseline before work begins on the
next manufacturer. The accompanying CLI, `tools/report_aco_final_baseline.py`,
is read-only: it reads an already exported canonical workbook, validates the
protected state, exercises the approved conditional customer projections, and
compares the workbook SHA-256 before and after the audit.

This handoff does **not** authorize changes to production generation, canonical
data, workbook schemas, scoring inputs, readiness flags, customer policies, or
protected technical values. Easyflow and ShowerDrain E+ remain explicitly
unpromoted.

A successful final audit ends with:

```text
OVERALL: ACO_BASELINE_STABLE
unresolved_nonblocking:
  - Easyflow article-level attribution
  - E+ explicit compatibility evidence
recommended_next_step:
  - freeze ACO baseline and begin next manufacturer integration
```

## Canonical workbook counts

| Sheet | Protected rows |
|---|---:|
| `Products` | 88 |
| `Comparison` | 88 |
| `Scoring_Field_Coverage` | 88 |
| `Candidates_All` | 118 |
| `Components` | 100 |
| `BOM_Options` | 251 |
| `Final_Assemblies` | 62 |
| `Final_Set_Details` | 62 |
| `Article_Variants` | 76 |
| `Mplus_Compound_Mappings` | 4 |
| `Eplus_Proposal_Mappings` | 3 |
| `Eplus_Compatible_Grate_Evidence` | 3 |
| `Cplus_Compatible_Grate_Evidence` | 30 |
| `Bline_Source_Evidence` | 8 |
| `Conditional_Technical_Values` | 24 |
| `Scoring_Scenarios` | 3 |
| `Comparison_flow_head_10mm` | 88 |
| `Comparison_flow_head_20mm` | 88 |

`Final_Set_Details` has exactly **56** canonical customer-ready rows: 6
EasyflowPlus + 16 ShowerDrain S+ + 4 ShowerDrain C + 30 ShowerDrain C+.

## Family-by-family final status

| Family | Canonical model and count | Benchmark/customer status | Protected conclusion |
|---|---|---|---|
| Easyflow | 2 generated assemblies | Blocked / blocked | Partial rows; `flow_rate_lps`, `height_adj_min_mm`, and `height_adj_max_mm` stay empty. Candidate articles `2500.00.00`, `2500.05.00`, and `2500.55.00` are retained, but none is selected. |
| EasyflowPlus | 6 complete assemblies | Ready / ready | Complete unconditional production family. |
| ShowerDrain S+ | 16 complete assemblies | Ready / ready | Complete unconditional production family. |
| ShowerDrain C | 4 complete assemblies | Ready / ready | Complete unconditional production family. |
| ShowerDrain C+ | 30 `base_x_grate` production assemblies | Ready / approved and enabled | Exactly 30 explicit compatibility-evidence rows and 30 compatible-grate BOM rows. Tile remains excluded. Protected hydraulics remain unchanged. |
| ShowerDrain M+ | 4 `channel_body_x_drain_body_x_grate` compound assemblies | Canonical blocked; selected runtime scenario ready | Canonical scalar flow stays empty. Runtime flow is 0.40 l/s at 10 mm and 0.46 l/s at 20 mm. No-selection is blocked; each selected scenario exposes 4 customer-ready rows. No scalar default exists. |
| ShowerDrain B | 8 direct `integral_all_in_one_set` products | Canonical blocked; selected runtime scenario ready | Direct finished sets, not generated `base_x_grate` assemblies. There are no B-line BOM, `Final_Assemblies`, or `Final_Set_Details` rows. Runtime flow is 0.40 l/s at 10 mm and 0.46 l/s at 20 mm; each selected scenario exposes 8 customer-ready rows. No scalar default exists. |
| ShowerDrain E+ | 3 proposals + 3 diagnostic evidence rows; 0 production assemblies | Blocked / blocked | Diagnostic-only. There is no explicit article-level body-to-grate compatibility matrix. |

## Approved customer behavior

- Unconditional customer presentation is enabled only for the 56 approved
  `Final_Set_Details` rows described above.
- All 30 C+ production assemblies are customer-approved and enabled.
- Easyflow remains at zero customer-ready rows.
- M+ remains at zero canonical customer-ready rows. With no flow-head selection,
  it is blocked; an explicit 10 mm or 20 mm selection resolves exactly 4 rows.
- B-line remains at zero canonical customer-ready rows. With no flow-head
  selection, it is blocked; an explicit 10 mm or 20 mm selection resolves
  exactly 8 direct finished-set rows.
- E+ remains at zero customer-ready rows.
- No M+ or B-line scalar/default flow may be introduced unless a separately
  reviewed source and policy change explicitly authorizes it.

## Protected C+ scope and hydraulics

Only the two approved bases and the 15 proven non-Tile design grates are in
scope. Each base has 15 explicit compatibility rows:

| Base | Flow | Water seal | Outlet | Height range |
|---|---:|---:|---|---:|
| `aco-showerdrain-cplus-standard-h92` | 0.91 l/s | 50 mm | DN50 | 80–128 mm |
| `aco-showerdrain-cplus-low-h69` | 0.62 l/s | 25 mm | DN40 | 57–128 mm |

The final audit rejects changed hydraulic values, an altered base scope, a
missing/extra explicit evidence row, a missing/extra compatible-grate BOM row,
or any Tile item in the approved C+ matrix.

## Unresolved nonblocking evidence gaps

These gaps are intentionally carried into the handoff. They do not destabilize
the approved ACO baseline because the affected families remain fail-closed.

### Easyflow: exact primary evidence still required

Obtain one primary-source ACO catalogue table row, technical datasheet row,
technical drawing, or product-page record that links **one exact canonical
Easyflow assembly/drain-body identity** to **one exact article number** from
`2500.00.00`, `2500.05.00`, or `2500.55.00`. The same source record must contain:

1. the exact drain-body or assembly identity;
2. the exact article number;
3. water seal and outlet DN;
4. article-specific flow rate;
5. article-specific installation or adjustment-height minimum and maximum;
6. the applicable side-inlet/variant condition; and
7. enough identity detail to exclude the other two candidates.

Family-page co-occurrence, row ordering, selecting the first/minimum/maximum
value, or combining technical values from separate source rows is insufficient.
Until the complete same-record link is available and separately approved, no
article, flow, or height may be promoted.

### ShowerDrain E+: exact primary evidence still required

Obtain an ACO catalogue compatibility table, technical manual/table, technical
drawing, or equivalent primary source that explicitly links each intended E+
**body article number** to each compatible E+ **grate article number**. The
body and grate article numbers and the explicit compatibility statement/matrix
must occur in the same source row, table cell relationship, or unambiguous
drawing relationship.

Separate body and grate listings, family-page co-occurrence, navigation pages,
or a generic statement that E+ supports design grates is insufficient. New
source material must first be recorded as diagnostic evidence; production
assembly generation requires a separate promotion review.

## Canonical export and validation commands

Run from the repository root on the Windows workstation that owns the canonical
Downloads output path:

```bash
python tools/export_canonical_aco_benchmark_xlsx.py --out "C:\Users\OCundr\Downloads\benchmark_output.xlsx"
python tools/validate_latest_xlsx_export.py --dir "C:\Users\OCundr\Downloads"
python tools/report_aco_final_baseline.py --xlsx "C:\Users\OCundr\Downloads\benchmark_output.xlsx"
```

Direct validation of a known file is also available:

```bash
python tools/validate_xlsx_export.py "C:\Users\OCundr\Downloads\benchmark_output.xlsx"
```

The exporter is fail-closed and must not publish a workbook when protected
counts or family production boundaries differ from the canonical baseline.

## Regression test commands

```bash
python -m pytest -q tests/test_report_aco_final_baseline.py
python -m pytest -q tests/test_pipeline_export.py tests/test_validate_xlsx_export.py
python -m pytest -q
```

## Important diagnostic commands

```bash
# Overall workbook quality and customer readiness
python tools/report_aco_xlsx_quality.py --xlsx "C:\Users\OCundr\Downloads\benchmark_output.xlsx"
python tools/report_customer_view_readiness.py --xlsx "C:\Users\OCundr\Downloads\benchmark_output.xlsx"

# Easyflow unresolved article attribution
python tools/report_easyflow_article_level_attribution.py --xlsx "C:\Users\OCundr\Downloads\benchmark_output.xlsx"
python tools/diagnose_easyflow_article_variants.py
python tools/diagnose_easyflow_assembled_fields.py

# C+ approved compatibility/publication scope
python tools/report_cplus_compatible_grate_evidence.py
python tools/report_cplus_customer_publication_review.py --xlsx "C:\Users\OCundr\Downloads\benchmark_output.xlsx"

# M+ compound mapping and conditional behavior
python tools/report_mplus_compound_assembly_mapping.py
python tools/report_mplus_conditional_customer_policy.py --xlsx "C:\Users\OCundr\Downloads\benchmark_output.xlsx"
python tools/report_mplus_production_readiness_gate.py

# B-line direct finished-set review
python tools/report_bline_source_evidence.py
python tools/report_bline_finished_set_production_review.py --xlsx "C:\Users\OCundr\Downloads\benchmark_output.xlsx"

# E+ proposal/evidence boundary
python tools/report_eplus_proposal_mappings.py
python tools/report_eplus_compatible_grate_evidence.py
```

Some diagnostic CLIs support live-source or repository-fixture modes rather
than `--xlsx`; use `python <tool> --help` before changing the invocation. The
final baseline CLI and canonical validator are the authoritative freeze gates.

## Branch strategy

- `integration-kaldewei-aco` is the integration base containing the completed
  Kaldewei and ACO work.
- `aco-final-baseline-audit` is a read-only audit/handoff branch based on that
  integration line. It should contain only the final audit CLI, regression
  tests, and documentation milestone—no production-generation or canonical-data
  changes.
- Merge this branch back into the integration line only after all tests and the
  Windows canonical export/validation/audit commands pass.
- Treat the resulting commit as the frozen ACO baseline. Any future Easyflow or
  E+ promotion must use a separate evidence-specific branch and must demonstrate
  that unrelated protected counts, values, and customer policies are unchanged.
- Start the next manufacturer on a new manufacturer-specific branch from the
  frozen integration baseline. Do not reuse an ACO unresolved-evidence branch.

## Recommended next-manufacturer onboarding sequence

1. **Freeze and archive ACO:** retain the validated workbook, validation output,
   final audit output, commit SHA, and source-evidence documents together.
2. **Create an isolated manufacturer branch:** branch from the frozen
   integration baseline and define the manufacturer/family scope before coding.
3. **Inventory primary sources:** capture official family pages, article pages,
   catalogues, technical sheets, compatibility matrices, and retrieval metadata.
4. **Add diagnostic-only extraction first:** preserve raw article identities,
   technical observations, source URLs, and uncertainty without generating
   production products or assemblies.
5. **Classify modelling boundaries:** distinguish direct complete sets,
   base/component systems, optional accessories, and family/navigation records.
6. **Prove article-level compatibility:** require explicit same-record matrices
   before generating combinatorial assemblies; never infer compatibility from
   co-occurrence or nominal dimensions alone.
7. **Preserve conditional values:** store condition-specific observations without
   inventing scalar defaults. Add runtime/scenario policy only after review.
8. **Add a small golden fixture and regression slice:** prove extraction,
   normalization, assembly rules, evidence lineage, and fail-closed behavior.
9. **Promote incrementally:** move only explicitly supported rows into canonical
   production frames, then review benchmark and customer readiness separately.
10. **Re-run all cross-manufacturer gates:** verify the new integration changes
    only the intended counts and leaves the frozen ACO classifications, protected
    hydraulics, conditional behavior, and customer publication state intact.

## New-conversation continuation brief

Use the following context when continuing in a new ChatGPT conversation:

> The repository is `Benchmarking-App-GPT`. The completed integration base is
> `integration-kaldewei-aco`; the final read-only ACO audit branch is
> `aco-final-baseline-audit`. ACO is frozen at Products/Comparison 88/88,
> Components 100, BOM 251, Final assemblies/details 62/62, and conditional
> values 24. Easyflow has 2 blocked partial assemblies with three unresolved
> candidate articles and no scalar flow/height. E+ has 3 proposals and 3
> diagnostic evidence rows but no production assemblies or explicit article
> compatibility matrix. C+ has 30 approved non-Tile assemblies with protected
> hydraulics. M+ has 4 conditional compound assemblies and B-line has 8 direct
> integral finished-set products; both resolve 0.40 l/s at 10 mm and 0.46 l/s
> at 20 mm only after explicit scenario selection, with no scalar default.
> Run the canonical exporter, validator, and
> `tools/report_aco_final_baseline.py --xlsx <workbook>` before starting the next
> manufacturer. Do not promote Easyflow/E+, change ACO counts, or introduce
> M+/B-line scalar defaults.
