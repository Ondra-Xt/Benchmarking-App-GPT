# Changelog - Current Stable ACO Benchmark Baseline

## Status

Stable and validated as of June 11, 2026.

This document records the canonical ACO benchmark baseline after the approved
ShowerDrain B direct finished-set production promotion. It is a documentation
snapshot of existing export behavior; it does not change product generation,
scoring, validation, readiness flags, scenario resolution, or XLSX output.

## Canonical workbook counts

| Sheet | Rows |
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

The stable canonical baseline is therefore **88 / 88 / 24** for `Products`,
`Comparison`, and `Conditional_Technical_Values`. The two explicit flow-head
scenario sheets each cover all 88 products.

## Current production and publication state

| Product family | Product/assembly rows | Current state |
|---|---:|---|
| Easyflow | 2 assembled rows | Partial/blocked; article-level flow and adjustable-height attribution remain ambiguous; not benchmark-ready or customer-view-ready |
| EasyflowPlus | 6 assembled rows | Complete and customer-view-ready |
| ShowerDrain C | 4 assembled rows | Complete and customer-view-ready |
| ShowerDrain S+ | 16 assembled rows | Complete and customer-view-ready |
| ShowerDrain M+ | 4 compound assembled rows | Conditional flow is scenario-resolvable, but default benchmark/customer publication remains blocked |
| ShowerDrain C+ | 30 production assemblies | Explicit catalogue matrix; benchmark-ready and customer publication approved |
| ShowerDrain E+ | 0 production assemblies | 3 proposal/evidence rows; diagnostic/proposal-only |
| ShowerDrain B | 8 direct finished-set products | Present in `Products`/`Comparison`; default benchmark/customer publication remains blocked pending conditional-flow policy |

`Final_Set_Details` contains 56 rows with `ready_for_customer_view=True`:
6 EasyflowPlus + 4 ShowerDrain C + 16 ShowerDrain S+ + 30 ShowerDrain C+.
Easyflow and M+ remain blocked there. B-line has no `Final_Set_Details` rows
because its promoted articles are direct finished-set products rather than
generated assemblies. E+ remains diagnostic-only.

## Milestone conclusions

### ShowerDrain B: eight direct integral finished sets promoted

Eight approved article-level ShowerDrain B products are now present in
`Products`, `Comparison`, `Scoring_Field_Coverage`, and both explicit flow-head
scenario sheets:

- `9010.78.70`
- `9010.78.71`
- `9010.78.72`
- `9010.78.73`
- `3018172`
- `3018173`
- `3018174`
- `3018175`

Their canonical classification is:

- `product_family = showerdrain_b`;
- `assembly_model = integral_all_in_one_set`;
- direct article-level finished-set products, not `base_x_grate` assemblies.

The product-page evidence describes each article as a complete set containing
its channel, grating, and gully. There is no separately selectable body × grate
matrix to model. Therefore the promotion intentionally creates:

- no B-line `BOM_Options` rows;
- no B-line `Final_Assemblies` rows;
- no B-line `Final_Set_Details` rows;
- no inferred body-to-grate compatibility;
- empty `body_article_number` and `grate_article_number` fields.

B-line flow remains condition-dependent: **0.40 l/s at 10 mm head** and
**0.46 l/s at 20 mm head**. The eight products contribute 16 rows to
`Conditional_Technical_Values`, and the explicit scenario sheets resolve the
applicable value. The scalar `flow_rate_lps` remains empty because the source
does not designate a default head condition. No unsupported default has been
selected, so default benchmark and customer publication remain blocked pending
an approved conditional-flow policy.

See [the B-line source-evidence audit](aco_bline_source_evidence_audit.md) for
the article classification, evidence boundary, and direct-promotion rationale.

### ShowerDrain C+: customer-approved production baseline

C+ remains production-ready from explicit article-level catalogue evidence:

- two protected hydraulic bases are each paired with 15 proven non-Tile grates,
  producing **2 × 15 = 30 assemblies**;
- `BOM_Options` contains 30 C+ compatible-grate rows;
- `Cplus_Compatible_Grate_Evidence` contains 30 explicit catalogue rows;
- `compatibility_evidence_type = explicit_catalog_matrix`;
- compatibility confidence is high and article-level compatibility is proven;
- `ready_for_benchmark = True`;
- `ready_for_customer_view = True`;
- `customer_view_enabled = True`.

Customer publication is approved for exactly these 30 assemblies. Tile articles
remain excluded, and protected hydraulic values remain unchanged.

See [the C+ source-evidence audit](aco_cplus_source_evidence_audit.md) for the
protected base mappings, catalogue source, and promotion boundary.

### ShowerDrain M+: conditional flow remains preserved

The four M+ compound assembly rows retain both source-backed observations:

- **0.40 l/s** at **10 mm** head;
- **0.46 l/s** at **20 mm** head.

No unconditional/default scalar `flow_rate_lps` is selected. M+ contributes
8 rows to `Conditional_Technical_Values`; the two explicit flow-head scenario
sheets resolve the corresponding values. The default benchmark/customer state
remains blocked because no default head condition has been approved.

See [the conditional parameter scoring review](conditional_parameter_scoring_review.md)
for the conditional-value preservation and scenario-resolution policy.

### ShowerDrain E+: blocked pending explicit compatibility evidence

E+ remains diagnostic/proposal-only because no explicit article-level
body-to-grate compatibility matrix has been found:

- `Eplus_Proposal_Mappings` contains 3 proposal rows;
- `Eplus_Compatible_Grate_Evidence` contains 3 diagnostic rows;
- no E+ production assemblies are generated;
- family-page co-occurrence is not accepted as article-level compatibility.

See [the E+ source-evidence audit](aco_eplus_source_evidence_audit.md) for the
search scope and blocking rationale.

### Easyflow: partial and blocked by article ambiguity

Easyflow retains two partial assembled rows, but the family-level base maps to
multiple article candidates:

- `2500.00.00`;
- `2500.05.00`;
- `2500.55.00`.

The candidates carry conflicting source-backed flow and adjustable-height
attribution, so a unique article and safe defaults cannot be selected.
`Article_Variants` remains an evidence-only sheet with 76 rows; its candidates
are not promoted into the canonical product universe. Easyflow is neither
benchmark-ready nor customer-view-ready.

The stricter second-stage record-level review and required promotion evidence are documented in [ACO Easyflow article-level attribution review v2](aco_easyflow_article_level_attribution_review_v2.md).

## Historical milestones retained

### C+ production assembly milestone (80-product baseline)

Before the B-line direct finished-set promotion, the approved C+ production
milestone had 80 rows in `Products`, `Comparison`, and
`Scoring_Field_Coverage`; 251 `BOM_Options`; and 62 rows in both
`Final_Assemblies` and `Final_Set_Details`. That milestone introduced the 30
catalogue-proven C+ assemblies while leaving B-line as evidence-only. The later
customer-publication approval enabled those same 30 C+ assemblies without
changing the assembly counts.

### Earlier final-assembly milestones

The 62 generated final assemblies remain unchanged by the B-line promotion:
2 Easyflow + 6 EasyflowPlus + 4 ShowerDrain C + 16 ShowerDrain S+ +
4 ShowerDrain M+ + 30 ShowerDrain C+. The B-line milestone expands the direct
product universe from 80 to 88 without creating generated assembly or BOM rows.

## Canonical validation gate

Run the canonical export and latest-workbook validator from the repository root
in Windows Command Prompt:

```bat
set PYTHONPATH=.
python tools/export_canonical_aco_benchmark_xlsx.py --out "C:\Users\OCundr\Downloads\benchmark_output.xlsx"
python tools/validate_latest_xlsx_export.py --dir "C:\Users\OCundr\Downloads"
```

Expected validator result:

```text
OVERALL: PASS
```

The gate covers canonical row counts, assembly-family counts, evidence sheets,
conditional M+ and B-line scenario resolution, and the C+, E+, Easyflow, and
B-line promotion boundaries described above.
