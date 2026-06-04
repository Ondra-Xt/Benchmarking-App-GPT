# Conditional Parameter Scoring and Comparison Export Review

## Scope and non-change confirmation

This document is a review-only proposal for conditional technical values in benchmark scoring and
XLSX Comparison output. It does **not** change production scoring, connector discovery, BOM
creation, final assembly creation, XLSX export behavior, customer-facing output, or baseline row
counts.

The current diagnostic baseline must remain unchanged until a later implementation explicitly
introduces scenario-aware scoring:

| Sheet / metric | Current baseline |
|---|---:|
| Products | 46 |
| Comparison | 46 |
| Scoring_Field_Coverage | 46 |
| Candidates_All | 118 |
| Components | 100 |
| BOM_Options | 221 |
| Final_Assemblies | 28 |
| Final_Set_Details | 28 |
| Mplus_Compound_Mappings | 4 |
| Eplus_Proposal_Mappings | 3 |
| Conditional_Technical_Values | 8 |
| Article_Variants | 76 |
| ShowerDrain S+ assembled | 16 |
| ShowerDrain C assembled | 4 |
| Easyflow assembled | 2 |
| Easyflow+ assembled | 6 |
| ShowerDrain M+ assembled | 0 |
| ShowerDrain E+ assembled | 0 |
| ShowerDrain C+ assembled | 0 |
| ShowerDrain B assembled | 0 |

## Current scoring behavior

### Scalar flow scoring

Current benchmark scoring is scalar-row based. `compute_parameter_score()` calls `_score_flow_rate()`
with the row dictionary, and `_score_flow_rate()` only reads `flow_rate_lps` from that same row. Missing,
empty, non-numeric, or non-positive values score `0.0` with pass status `unknown`; values greater than
or equal to `0.8` l/s score `1.0`; lower positive values score linearly as `flow / 0.8` and receive pass
status `no`.

Implications:

- Products with one scalar `flow_rate_lps` are scoreable today.
- Products missing scalar `flow_rate_lps` receive zero flow contribution and `flow_rate_pass_0_8_lps = unknown`.
- Conditional values in `Conditional_Technical_Values` are not read by scoring.
- Proposal-only diagnostic rows are not made production-ready by the presence of conditional values.

### Pipeline scalarization behavior

The ingestion pipeline contains legacy scalarization behavior for ACO rows that expose
`flow_rate_10mm_lps` / `flow_rate_20mm_lps` directly on extracted product parameters: when
`flow_rate_lps` is missing and either split field is numeric, the pipeline fills scalar `flow_rate_lps`
with the maximum split value. This behavior is separate from the M+ diagnostic-only conditional sheet
and must not be extended to M+ compound proposal rows.

For production eligibility, the pipeline still treats missing scalar `flow_rate_lps` as a missing required
part. Therefore a product or assembly with only conditional flow values remains blocked unless a future
policy explicitly maps a scenario to a scoreable value.

### Assembled products

Final assembly export uses scalar technical fields (`flow_rate_lps`, `water_seal_mm`, `outlet_dn`,
`height_adj_min_mm`, `height_adj_max_mm`) to determine completeness. `Final_Set_Details` marks rows
ready for benchmark/customer view only when data quality is `complete`. Partial Easyflow rows are
explicitly blocked because flow/height remain ambiguous at the current article/variant granularity.

### Proposal-only diagnostics

M+ compound mapping rows deliberately preserve flow as split diagnostic fields while leaving the scalar
`flow_rate_lps` and `selected_default_flow_rate_lps` empty. They are marked `safe_to_generate = False`,
`ready_for_benchmark = False`, `ready_for_customer_view = False`, and blocked by
`blocked_pending_conditional_parameter_scoring`.

## Current conditional technical value structure

`Conditional_Technical_Values` is currently a diagnostic-only normalized long-format sheet derived from
`Mplus_Compound_Mappings`. Each M+ set receives two rows for `parameter_name = flow_rate_lps`:

| condition_type | condition_value | value | unit | condition_label |
|---|---:|---:|---|---|
| head_water_level | 10 | 0.40 | l/s | 10 mm head water level |
| head_water_level | 20 | 0.46 | l/s | 20 mm head water level |

The sheet also carries set identity, family, assembly model, component IDs, source URLs, evidence type,
confidence, attribution scope, article-specific flag, production blocking state, and recommended next
action. This is the correct preservation shape: it does not collapse multiple condition-dependent values
into one default and does not discard either value.

## Recommended scoring architecture for conditional parameters

### 1. Represent technical values as value observations

Introduce an internal value-observation layer, not a single-purpose M+ flow exception. A value observation
should be able to describe scalar, conditional, variant-specific, and range-based evidence:

| Field | Purpose |
|---|---|
| `entity_id` | Product, set, component, or variant ID being scored. |
| `parameter_name` | Canonical parameter, e.g. `flow_rate_lps`, `height_adj_min_mm`, `load_class`. |
| `value` / `unit` | Numeric or categorical value and unit. |
| `condition_type` | E.g. `head_water_level`, `length_mm`, `drain_body_id`, `grate_id`, `variant_id`. |
| `condition_value` / `condition_unit` | Condition value and unit where applicable. |
| `condition_label` | Human-readable condition. |
| `scope` | Product-level, set-level, component-level, family-level, or variant-level attribution. |
| `evidence` | Source URL, evidence type, confidence, article-specific flag. |
| `production_status` | Whether the observation is scoreable, diagnostic-only, or blocked. |

The existing `Conditional_Technical_Values` shape is already close to this normalized observation model.
Future implementation should generalize that sheet rather than special-case M+.

### 2. Add scenario-based value resolution

Scoring should not read conditional observations directly. It should first resolve a scenario-specific
score input through a value resolver. Recommended scenario IDs:

| Scenario | Resolution rule | Intended use |
|---|---|---|
| `flow_head_10mm` | Select `flow_rate_lps` where `condition_type = head_water_level` and `condition_value = 10 mm`; scalar rows remain usable if explicitly declared comparable to this scenario. | Like-for-like low-head comparison. |
| `flow_head_20mm` | Select `flow_rate_lps` at 20 mm head. | Like-for-like higher-head comparison. |
| `conservative_minimum` | Use the minimum declared value across valid observations for that parameter and entity. | Safety-first single-score view. |
| `maximum_declared` | Use the maximum declared value, clearly labeled as such. | Marketing/upper-bound diagnostic view, not default production scoring. |
| `no_scenario_selected` | Do not resolve conditional observations to scalar values; mark conditional rows as scoring-blocked. | Backward-compatible default until policy acceptance. |

Default recommendation: keep `no_scenario_selected` as the production default until product owners accept
a scenario policy. This preserves current behavior and prevents accidental selection of M+ `0.46` l/s or
`0.40` l/s as a hidden default.

### 3. Make scenario policy explicit in score outputs

Every score that used conditional data should carry metadata:

- `scoring_scenario_id`
- `scoring_scenario_label`
- `resolved_parameter_name`
- `resolved_parameter_value`
- `resolved_parameter_unit`
- `resolved_condition_type`
- `resolved_condition_value`
- `resolved_condition_unit`
- `conditional_resolution_policy`
- `conditional_resolution_warning`

The score is only production-ready when this policy is non-empty, accepted, and visible in export.

## Recommended Comparison/XLSX representation

A hybrid export is recommended. It keeps the current diagnostic long-format sheet as the canonical
source of conditional values and adds scenario-specific views only when scenario scoring is implemented.

### Options evaluated

| Option | Pros | Cons | Recommendation |
|---|---|---|---|
| Separate `Conditional_Technical_Values` sheet only | Already normalized; preserves all conditions; avoids changing Comparison baseline. | Users must join sheets to understand conditional values next to scores. | Keep as canonical diagnostic/export source. |
| Additional `Comparison` columns such as `flow_rate_lps_10mm_head` and `flow_rate_lps_20mm_head` | Easy for users to scan common flow scenarios. | Does not generalize to arbitrary condition types; can explode column count; risks implying all values are comparable. | Add later only for a small curated set of high-value scenarios, clearly labeled. |
| Scenario-specific `Comparison` sheets | Preserves the familiar wide Comparison format while making scenario selection explicit. | More sheets; requires clear naming and validator coverage. | Recommended for production scenario scoring, e.g. `Comparison_flow_head_10mm`. |
| Normalized long-format comparison rows | Most general for future condition dimensions and BI tools. | Less friendly for current customer-facing Comparison workflow. | Recommended as an additional diagnostic sheet, not the only view. |
| Hybrid | Balances backward compatibility, readability, and generality. | More implementation work. | Recommended. |

### Proposed future workbook layout

Phase-ready workbook layout once conditional scoring is implemented:

1. Keep existing `Comparison` unchanged unless a user explicitly requests a scenario.
2. Keep `Conditional_Technical_Values` as the canonical long-format observation table.
3. Add `Scoring_Scenarios` documenting each scenario ID, parameter, condition filters, aggregation rule,
   readiness impact, and owner-approved status.
4. Add `Comparison_<scenario_id>` sheets for approved scenarios only.
5. Optionally add curated helper columns to `Comparison`, such as `flow_rate_lps_10mm_head` and
   `flow_rate_lps_20mm_head`, but keep score columns scenario-neutral unless explicitly selected.

## Recommended Scoring_Field_Coverage changes

Current coverage answers whether `has_flow_rate_lps` is present as a scalar positive number. It should be
extended in a future implementation to distinguish scalar coverage from conditional coverage.

Recommended fields:

| Field | Meaning |
|---|---|
| `has_flow_rate_lps` | Current scalar-positive check; preserve for backward compatibility. |
| `has_conditional_flow_rate_lps` | True when conditional observations exist for `flow_rate_lps`. |
| `flow_rate_status` | `scalar`, `conditional`, `scalar_and_conditional`, `missing`, or `not_applicable`. |
| `conditional_parameter_names` | Comma-separated conditional parameters available for the row/entity. |
| `conditional_condition_types` | Comma-separated condition dimensions, e.g. `head_water_level`. |
| `scenario_scoreable_fields` | Fields resolvable under the selected scenario. |
| `scenario_blocked_fields` | Conditional fields not resolvable because no scenario/policy was selected. |
| `present_scoring_fields` | Keep existing field; optionally include scenario-resolved fields only when policy is selected. |
| `missing_scoring_fields` | Keep existing field; for M+ under `no_scenario_selected`, keep `flow_rate_lps` missing. |

For current M+ rows before scenario scoring, the correct classification is:

- `has_flow_rate_lps = False`
- `has_conditional_flow_rate_lps = True`
- `flow_rate_status = conditional`
- `missing_scoring_fields` still includes `flow_rate_lps` for production scoring readiness
- readiness remains blocked because no scenario policy is implemented

## Recommended readiness/status vocabulary

Use status vocabulary that separates data richness from scoring readiness:

| Status / flag | Meaning |
|---|---|
| `conditional_parameter_available_production_blocked` | Conditional values exist and are preserved, but production generation/scoring is blocked. |
| `technically_data_rich_scoring_blocked` | Data is sufficient to support future scenario scoring but is not scalar-scoreable today. |
| `diagnostic_only_until_scenario_scoring` | Exported only for review and diagnostics; not customer-facing. |
| `blocked_pending_conditional_parameter_scoring` | Required scenario scoring/resolution policy is not implemented or accepted. |
| `scenario_scoreable` | A selected and approved scenario can resolve all required conditional scoring inputs. |
| `scenario_scored` | Row has been scored under a named scenario and score metadata is exported. |
| `not_production_ready` | Conservative umbrella status when any production guardrail fails. |

For M+ today, use `technically_data_rich_scoring_blocked` conceptually, with current concrete fields
remaining `ready_for_benchmark = False`, `safe_to_generate = False`, and
`blocking_reason = blocked_pending_conditional_parameter_scoring`.

## Migration plan for M+ conditional flow values

1. **Document and freeze the diagnostic baseline.** Keep Products, Comparison, Final_Assemblies,
   Final_Set_Details, Mplus_Compound_Mappings, and Conditional_Technical_Values counts unchanged.
2. **Introduce internal conditional observation objects.** Use the existing M+ conditional rows as fixture
   data; do not feed observations into scoring yet.
3. **Add coverage diagnostics.** Extend coverage in diagnostic mode to report scalar vs conditional vs
   missing flow status.
4. **Implement a scenario resolver behind a feature flag.** Start with `no_scenario_selected`,
   `flow_head_10mm`, `flow_head_20mm`, `conservative_minimum`, and `maximum_declared`.
5. **Add scenario score tests.** Verify that M+ produces different flow inputs for 10 mm and 20 mm, and
   that `no_scenario_selected` remains blocked.
6. **Add scenario-specific Comparison sheets.** Keep current `Comparison` untouched until scenario output
   is explicitly enabled.
7. **Update validators.** Add opt-in validator expectations for scenario sheets and metadata while keeping
   current baseline validator expectations unchanged by default.
8. **Promote M+ only after policy acceptance.** M+ production assemblies may move above zero only after
   scenario scoring, export metadata, and readiness gates all pass.

## Risks and guardrails

| Risk | Guardrail |
|---|---|
| Accidentally choosing a hidden default flow value. | Keep `selected_default_flow_rate_lps` empty unless an accepted policy is recorded. |
| Inflating scores by using maximum declared values. | Make `maximum_declared` diagnostic or explicitly policy-approved, never implicit default. |
| Comparing scalar products against conditional products at different assumptions. | Require `scoring_scenario_id` and scenario metadata on every scenario score. |
| Wide Comparison column sprawl. | Keep normalized `Conditional_Technical_Values` canonical; add only curated helper columns. |
| Future parameters do not fit flow-specific logic. | Model observations by `parameter_name` and `condition_type`, not by M+ column names. |
| Validator drift. | Preserve current default expected counts; add scenario validator checks only under an explicit mode. |
| Customer confusion. | Keep diagnostic-only conditional sheets out of customer-ready views until scenario semantics are clear. |

## Proposed implementation phases

### Phase 0: Review-only documentation

- Add this review document.
- Make no production behavior changes.
- Keep all current baseline counts and validator expectations unchanged.

### Phase 1: Diagnostic model and coverage

- Add reusable conditional observation helpers.
- Add read-only diagnostics for conditional parameter counts by family, parameter, condition type, and set.
- Optionally extend `Scoring_Field_Coverage` in a diagnostic-only mode.

### Phase 2: Scenario resolver

- Implement scenario selection and value resolution without changing default scoring behavior.
- Add tests for `no_scenario_selected`, `flow_head_10mm`, `flow_head_20mm`, `conservative_minimum`, and
  `maximum_declared`.

### Phase 3: Scenario exports

- Add `Scoring_Scenarios` and `Comparison_<scenario_id>` sheets behind explicit configuration.
- Add validator support for scenario sheets in opt-in mode.

### Phase 4: Production readiness gate

- Update readiness gates so M+ can become production-ready only when a scenario policy is selected,
  visible, tested, and accepted.
- Promote M+ assemblies only after explicit approval.

### Phase 5: Generalization

- Apply the same model to height ranges by variant, DN by drain body, load class by grate, material/finish
  by design variant, and length-dependent technical values.

## Files that would need to change in a future implementation

Likely production implementation files:

- `src/scoring.py` — add scenario-aware value resolution before scalar scoring.
- `src/pipeline.py` — pass scenario context and conditional observations without implicit scalar defaults.
- `src/excel_export.py` — add scenario metadata sheets, scenario-specific Comparison sheets, and future
  coverage/status columns.
- `tools/validate_xlsx_export.py` — add opt-in scenario/conditional coverage validation while preserving
  current baseline defaults.
- `tools/report_assembly_gaps.py` — classify conditionally data-rich but scoring-blocked families.
- `tools/report_mplus_production_readiness_gate.py` — distinguish data-rich/scoring-blocked from true
  production readiness.
- Tests covering scoring, pipeline export, XLSX validation, M+ readiness, assembly gaps, and conditional
  value preservation.

## Explicit production non-changes in this pass

This pass intentionally does not:

- select a default M+ flow value;
- write `0.40` or `0.46` into M+ production `flow_rate_lps` rows;
- alter scoring behavior;
- alter connector discovery;
- alter BOM or final assembly generation;
- alter XLSX export behavior;
- alter customer-facing output;
- alter baseline row-count expectations.
