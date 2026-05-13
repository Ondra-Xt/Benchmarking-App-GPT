# ACO ShowerDrain S+ PDF/Table Extraction Audit

## 1) Executive summary
**Can S+ assembled systems be created after this extraction?** **Partial / not yet fully safe**.

Official ACO sources provide strong evidence that ShowerDrain S+ is modular (profile + Ablaufkörper) and expose meaningful technical values for at least the Ablaufkörper branch. However, a full article-to-article compatibility matrix (profile/article ↔ drain-body/article) is still not conclusively extracted in a machine-safe form from currently inspected official pages/PDF references.

---

## 2) Official PDFs/tables checked

| Source | Type | What was extracted | Gaps |
|---|---|---|---|
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ | Family product page | Modular wording (profile + Ablauf), S+ family context, product structure hints. | No complete normalized article-compatibility matrix in inspected view. |
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/ | Detail page (Ablaufkörper) | Explicit “zu … ShowerDrain S+” relation; DN/flow/water-seal/DIN context evidence in official wording. | Full per-article profile↔drain mapping not fully extracted. |
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/ | Category page | Official S+ family placement in shower channels. | Not a technical article table source. |
| https://www.aco-haustechnik.de/downloads/ | Download index | Official source of brochures/datasheets. | Directly parseable S+ compatibility table not confirmed in this pass. |
| https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Duschrinne_ShowerDrain_S-Plus.pdf | Official brochure PDF | S+ product/brochure source confirmed. | Stable extracted article compatibility matrix still missing. |
| https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Badentwaesserung_Linie.pdf | Official line brochure PDF | Family-level technical comparisons/signals available. | Article-level compatibility mapping remains incomplete. |

---

## 3) Extracted profile/channel article table

> Result: **No complete article-level table confirmed in this pass** (family-level + brochure-level structure found, but not full machine-safe profile article matrix).

| Role | Article no. | Product name | Length | Material | Sealing / W3-I | Source | Confidence | Notes |
|---|---|---|---|---|---|---|---|---|
| Profile/channel/base | Not fully extracted | ShowerDrain S+ profile/channel family | Not fully extracted | Family-level indications only | W3-I context appears at family/brochure level | S+ family page + brochures | Medium-Low | Requires dedicated table/PDF parsing pass for article-normalized rows. |

---

## 4) Extracted drain body article table

| Role | Article no. | Product name | water_seal_mm | flow_rate_lps | flow_rate_10mm_lps | flow_rate_20mm_lps | outlet_dn | outlet_orientation | height range | DIN EN 1253 evidence | Source | Confidence |
|---|---|---|---:|---:|---:|---:|---|---|---|---|---|---|
| Drain body (Ablaufkörper to S+) | Not fully normalized in this pass | Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+ | 50 (WS50 branch evidence) | 0.8 (from 20mm evidence branch) | 0.7 | 0.8 | DN50 | Horizontal/waagerecht evidence | Not fully extracted | DIN EN 1253-1 context present | Ablaufkörper detail page | High (for visible branch values), Medium (for complete variant set) |
| Drain body (alternate branch) | Not fully normalized | Ablaufkörper … S+ | 30 mentioned in branch text | Incomplete in extracted snippet | Incomplete | Incomplete | DN50 page-level context | Likely horizontal | Not fully extracted | DIN context on page | Ablaufkörper detail page | Medium |

---

## 5) Compatibility matrix (current extraction state)

| Base/profile family/article | Compatible drain-body family/article | Exact wording / excerpt | Source | Confidence |
|---|---|---|---|---|
| ShowerDrain S+ profile family | S+ Ablaufkörper family | “Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+” | Ablaufkörper detail page | High |
| S+ modular system | Profile + Ablauf relation | “Baukastensystem bestehend aus Duschrinnenprofil und Ablauf” | S+ family page | High |

**Missing for full assembly safety:** explicit article-number-level mapping from each profile/channel article to each compatible drain-body article.

---

## 6) Technical parameter table (source-backed status)

| Parameter | Evidence status | Best source | Notes |
|---|---|---|---|
| flow_rate_lps | Partial | S+ Ablaufkörper detail page | Branch-specific values visible; needs article-normalized extraction. |
| flow_rate_10mm_lps | Partial | S+ Ablaufkörper detail page | Explicit in at least one branch; full variant coverage missing. |
| flow_rate_20mm_lps | Partial | S+ Ablaufkörper detail page | Explicit in at least one branch; full variant coverage missing. |
| water_seal_mm | Partial-Strong | S+ Ablaufkörper detail page | WS values visible (e.g., 50 and 30 branches), but full article matrix incomplete. |
| outlet_dn | Strong | S+ Ablaufkörper detail page | DN50 context present. |
| outlet_orientation | Partial | S+ Ablaufkörper detail page | Horizontal context present; variant table normalization incomplete. |
| height_adj_min/max | Weak | S+ pages/PDF | Not conclusively extracted in this pass. |
| material | Weak-Partial | Family/PDF content | Family-level indications only in current extraction. |
| sealing fleece / W3-I | Partial | Family/PDF/page text | Evidence exists, article-level normalization incomplete. |
| DIN EN 1253 | Partial-Strong | Ablaufkörper detail page | Explicit DIN EN 1253-1 wording present. |

---

## 7) Missing evidence blocking full assembled-system creation

1. **Article-level compatibility matrix missing**:
   - profile/article → compatible drain-body/article mapping not fully extracted.
2. **Complete article tables missing for profile/channel branch**:
   - lengths/material/technical values not fully normalized by article number.
3. **Incomplete full-variant technical normalization for drain bodies**:
   - some branch values visible, but complete per-article coverage not yet proven.

---

## 8) Recommendation
**Recommendation:** **Defer full assembled product implementation for now**.

Suggested next step (implementation-prep only):
1. Add a dedicated extraction helper (offline/audit script) for official S+ PDFs/tables.
2. Normalize article numbers and explicit compatibility links/matrices.
3. Only after matrix is complete, add assembled-system rows in production logic.

At current confidence, **BOM hints may be acceptable**, but **full benchmark assembled systems are not yet fully safe**.

---

## 9) Regression guardrails
Any future implementation must preserve:

- Candidates_All >= 69
- Products >= 27
- Components >= 54
- Comparison >= 27
- BOM_Options >= 81 unless bad rows are intentionally removed
- Scoring_Field_Coverage >= 27
- ShowerDrain C monitored products remain in Products and Comparison
- ShowerDrain C water_seal_mm remains 25 / 25 / 50 / 50
- no non-ACO connector changes
- no scoring formula changes

