# Changelog – Final Assemblies and Article Variants

## Status

Stable and validated.

This change set adds final assembled-product reporting, article-level evidence reporting, and validation coverage without changing the default benchmark universe.

Default benchmark counts remain unchanged:

- Products: 46
- Comparison: 46
- Scoring_Field_Coverage: 46
- Candidates_All: 118
- Components: 100
- BOM_Options: 221
- Final_Assemblies: 28

The XLSX validator reports OVERALL: PASS.

---

## Added

### Final_Assemblies XLSX sheet

Added a dedicated Final_Assemblies sheet to the XLSX export.

The sheet contains only final assembled products from Products, identified by product_id values starting with `aco-assembled-`.

Current family counts:

- easyflow: 2
- easyflowplus: 6
- showerdrain_c: 4
- showerdrain_splus: 16

Total Final_Assemblies rows: 28.

---

### Final_Assemblies completeness metadata

Added data-quality helper columns to Final_Assemblies:

- is_complete_technical_data
- missing_technical_fields
- data_quality_status
- source_status_note

Current status distribution:

- complete: 26
- partial: 2
- missing: 0

The two partial rows are Easyflow assembled products.

Easyflow assembled rows currently have:

- water_seal_mm = 50
- outlet_dn = DN50

Easyflow assembled rows intentionally still have empty:

- flow_rate_lps
- height_adj_min_mm
- height_adj_max_mm

Reason: flow and height values are ambiguous at the current article/variant granularity.

---

### Article_Variants XLSX sheet

Added a dedicated Article_Variants sheet to expose source-backed article-level evidence without changing the default benchmark universe.

The sheet includes normalized article-level rows with these columns:

- manufacturer
- base_product_id
- article_number
- variant_type
- product_family
- source_url
- water_seal_mm
- outlet_dn
- flow_rate_lps
- height_adj_min_mm
- height_adj_max_mm
- cutout_mm
- side_inlet
- row_text
- attribution_status
- why_not_promoted

Current Article_Variants state:

- Article_Variants exists
- Article_Variants rows: 76
- Required Easyflow WS50/DN50 article candidates are present:
  - 2500.55.00
  - 2500.05.00
  - 2500.00.00
- Article-level values are kept as evidence only
- No article variants are promoted to Products in default mode

---

### Easyflow article-variant diagnostics

Added diagnostics for Easyflow article/variant attribution.

The diagnostics confirm that the current Easyflow base row `aco-easyflow-komplettablaeufe-aco-easyflow-dn-50` matches multiple possible WS50/DN50 article candidates.

Relevant candidates include:

- 2500.55.00
- 2500.05.00
- 2500.00.00

These candidates have different source-backed flow values, including:

- 1.5 l/s
- 1.0 l/s

Because multiple article candidates match the current family-level base row, there is no unique article attribution.

Result: Easyflow flow_rate_lps and height fields remain unsafe to write into Products by default.

---

### Feature flag for article-variant products

Added a disabled-by-default config flag:

- enable_article_variant_products = False

Default behavior:

- Article variants are not promoted to Products
- Products remains 46
- Comparison remains 46
- BOM_Options remains 221
- Final_Assemblies remains 28
- Article_Variants remains evidence-only

Experimental behavior when enabled:

- Article-level Easyflow products can be generated from candidate Article_Variants rows
- Expected experimental product IDs include:
  - aco-easyflow-article-25005500
  - aco-easyflow-article-25000500
  - aco-easyflow-article-25000000

This mode is disabled by default and does not affect standard exports.

---

## Changed

### Easyflow assembled WS/DN inheritance

Easyflow assembled products now inherit source-backed technical fields from their resolved base row where safe.

The two Easyflow assembled rows now correctly receive:

- water_seal_mm = 50
- outlet_dn = DN50

They still intentionally do not receive:

- flow_rate_lps
- height_adj_min_mm
- height_adj_max_mm

Those values remain empty because the current base row cannot be uniquely attributed to a single article variant.

---

### XLSX validator coverage

The XLSX validator now checks:

- Required workbook sheets
- Existing baseline row counts
- Final_Assemblies existence
- Final_Assemblies row count
- Final_Assemblies family counts
- Final_Assemblies completeness metadata
- Easyflow partial status
- Article_Variants existence
- Article_Variants required columns
- Article_Variants source URLs
- Required Easyflow WS50/DN50 article candidates
- No article variant promotion into Products
- BOM integrity
- ACO dangling component references
- C+ baseline values

The latest validated XLSX reports OVERALL: PASS.

---

## Not Changed

The default benchmark universe remains unchanged.

No default changes were made to:

- Products count
- Comparison count
- Scoring_Field_Coverage count
- BOM_Options count
- Final_Assemblies count
- Product generation behavior
- BOM generation behavior
- Scoring formulas
- Connector discovery behavior

Article variants are not promoted to Products unless the experimental feature flag is explicitly enabled.

---

## Known Limitations

### Easyflow article ambiguity

Easyflow remains partially complete in Final_Assemblies.

Reason: the current Easyflow base row is family-level and matches multiple WS50/DN50 article candidates.

Because of this, the following fields remain empty for Easyflow assembled rows:

- flow_rate_lps
- height_adj_min_mm
- height_adj_max_mm

These values should not be filled until the model can safely distinguish article-level variants.

---

## Validation Summary

Latest validated state:

- Products: 46
- Comparison: 46
- Scoring_Field_Coverage: 46
- Candidates_All: 118
- Components: 100
- BOM_Options: 221
- Final_Assemblies: 28
- Article_Variants: present
- Final_Assemblies complete rows: 26
- Final_Assemblies partial rows: 2
- Final_Assemblies missing rows: 0
- Article variants promoted to Products: 0
- ACO dangling component_id: 0
- XLSX validation: OVERALL PASS

---

## Next Possible Work

### Option 1: Keep Article_Variants as evidence-only

Recommended for stable baseline.

Article variants remain visible in Excel, while Products and Comparison stay unchanged.

### Option 2: Improve article-level attribution

Add stronger mapping between family-level Easyflow base rows and concrete article variants.

This is required before safely filling Easyflow flow and height values.

### Option 3: Experimental variant-level Products

Use the disabled feature flag enable_article_variant_products = True to test article-level Easyflow products without changing the default benchmark export.

This should remain experimental until a new baseline and validator mode are explicitly defined.