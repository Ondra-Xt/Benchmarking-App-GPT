
# Changelog - Current Stable ACO Benchmark Baseline

## ACO C+ Production Assembly Milestone

Validated state:
- Products: 80
- Comparison: 80
- Scoring_Field_Coverage: 80
- Candidates_All: 118
- Components: 100
- BOM_Options: 251
- Final_Assemblies: 62
- Final_Set_Details: 62
- Cplus_Compatible_Grate_Evidence: 30
- C+ assembled: 30
- C+ BOM rows: 30
- XLSX validation: OVERALL PASS

Canonical export command:
python tools/export_canonical_aco_benchmark_xlsx.py --out "C:\Users\OCundr\Downloads\benchmark_output.xlsx"

Validation command:
python tools/validate_latest_xlsx_export.py --dir "C:\Users\OCundr\Downloads"

# Changelog – Final Assemblies and Article Variants


## Status

Stable and validated as of June 9, 2026.

This document records the canonical ACO benchmark baseline after the C+, E+,
Easyflow, and B-line milestones. It is a documentation snapshot of the existing
export behavior; it does not change product generation, scoring, validation, or
XLSX output.

## Canonical workbook counts

| Sheet | Rows |
|---|---:|
| `Products` | 80 |
| `Comparison` | 80 |
| `Scoring_Field_Coverage` | 80 |
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
| `Conditional_Technical_Values` | 8 |
| `Scoring_Scenarios` | 3 |
| `Comparison_flow_head_10mm` | 80 |
| `Comparison_flow_head_20mm` | 80 |

These are the stable canonical counts. Documentation work must not change them,
and no XLSX export behavior is changed by this changelog update.

## Assembled-product baseline

| Product family | Assemblies | Current status |
|---|---:|---|
| Easyflow | 2 | Partial/blocked |
| EasyflowPlus | 6 | Assembled |
| ShowerDrain C | 4 | Assembled |
| ShowerDrain S+ | 16 | Assembled |
| ShowerDrain M+ | 4 | Conditional/default benchmark blocked |
| ShowerDrain C+ | 30 | Production-ready; manual customer-view approval granted |
| ShowerDrain E+ | 0 | Diagnostic/proposal-only |
| ShowerDrain B | 0 | Diagnostic-only finished-set evidence |

The 62 production assembly rows are therefore 2 Easyflow + 6 EasyflowPlus +
4 ShowerDrain C + 16 ShowerDrain S+ + 4 ShowerDrain M+ + 30 ShowerDrain C+.
E+ and B-line evidence is deliberately not promoted to production assemblies.

## Milestone conclusions

### ShowerDrain C+: production-ready from an explicit catalogue matrix

C+ is production-ready from explicit article-level catalogue evidence:

- Two protected hydraulic bases are each paired with 15 proven grates,
  producing **2 x 15 = 30 assemblies**.
- Only catalogue-proven, length-compatible grate rows are promoted.
- Tile articles are not promoted.
- `Cplus_Compatible_Grate_Evidence` contains 30 rows.
- The customer view remains disabled even though the benchmark production gate
  is satisfied.

See [the C+ source-evidence audit](aco_cplus_source_evidence_audit.md) for the
protected base mappings, catalogue source, and promotion boundary.

### ShowerDrain M+: conditional flow is preserved

M+ retains both source-backed conditional flow observations:

- **0.40 l/s** at **10 mm** head;
- **0.46 l/s** at **20 mm** head.

No unconditional/default scalar `flow_rate_lps` is selected. The canonical
`Conditional_Technical_Values` sheet preserves the observations, and the
`Comparison_flow_head_10mm` and `Comparison_flow_head_20mm` scenario sheets
resolve the applicable value. The four M+ assemblies remain blocked from a
default benchmark result because the source does not identify a default head
condition.

See [the conditional parameter scoring review](conditional_parameter_scoring_review.md)
for the scenario-resolution policy.

### ShowerDrain E+: blocked pending explicit compatibility evidence

E+ remains diagnostic/proposal-only because no explicit article-level
body-to-grate compatibility matrix has been found:

- `Eplus_Proposal_Mappings` contains 3 proposal rows.
- `Eplus_Compatible_Grate_Evidence` contains 3 blocked rows.
- No E+ production assemblies are generated.
- Family-page co-occurrence is not accepted as article-level compatibility.

See [the E+ source-evidence audit](aco_eplus_source_evidence_audit.md) for the
search scope and blocking rationale.

### Easyflow: partial and blocked by article ambiguity

Easyflow retains two partial assembled rows, but the family-level base maps to
multiple article candidates:

- `2500.00.00`;
- `2500.05.00`;
- `2500.55.00`.

The candidates carry conflicting source-backed flow values, so a unique article
and a safe default flow cannot be selected. `Article_Variants` remains an
**evidence-only** sheet with 76 rows; its candidates are not promoted into the
canonical product universe.

### ShowerDrain B / B-line: diagnostic finished-set evidence only

B-line has explicit article evidence for eight integral all-in-one finished
sets:

- `9010.78.70`-`9010.78.73` without a sealing sleeve;
- `3018172`-`3018175` with an attached sealing sleeve.

These articles are complete finished sets, not separately selectable
`base_x_grate` assemblies. Consequently:

- `Bline_Source_Evidence` contains 8 diagnostic rows;
- no B-line production assemblies are generated;
- no body-to-grate compatibility is inferred;
- flow remains conditional at **0.40 l/s at 10 mm build-up** and **0.46 l/s at
  20 mm build-up**, with no unconditional default scalar.

See [the B-line source-evidence audit](aco_bline_source_evidence_audit.md) for
article classification and the production gate.

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

The gate covers the canonical row counts, assembly-family counts, evidence
sheets, conditional M+ scenario resolution, and the C+, E+, Easyflow, and
B-line promotion boundaries described above.
