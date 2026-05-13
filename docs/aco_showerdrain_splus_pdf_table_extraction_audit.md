# ACO ShowerDrain S+ PDF/Table Extraction Audit (Dedicated Pass)

## 1) Executive summary
**Can S+ assembled systems be created after this extraction pass?** **Partial / no (not yet safely)**.

This pass confirms official modular wording and confirms that official S+ Ablaufkörper sources expose technical values (flow/water-seal/DN/DIN context).  
However, this pass still does **not** produce a complete article-number-level compatibility matrix (`profile/channel article -> drain-body article`) from official S+ tables/PDFs that can be safely wired into benchmark assembled rows.

---

## 2) Official PDFs/tables checked

| URL | Type | Extraction target | What was found | What was still missing |
|---|---|---|---|---|
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ | Official family page | Profile/channel articles + system structure | Explicit modular/system wording for S+ family (profile + Ablauf); family structure visible. | Stable per-article profile table and explicit article-by-article compatibility mapping not extracted in a machine-safe way. |
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/ | Official detail page | Drain-body articles + hydraulics | Explicit “zu … ShowerDrain S+” relation; explicit hydraulic evidence exists (10/20 mm branch values in source text), water-seal branch values, DN50 context, DIN EN 1253-1 context. | Full normalized drain-body article table with complete profile compatibility links still incomplete. |
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/ | Official category page | Family coverage + table links | S+ appears as official family in category scope. | Not a direct article compatibility source. |
| https://www.aco-haustechnik.de/downloads/ | Official download index | Locate technical PDFs/tables | Official download hub confirmed. | No fully normalized S+ compatibility matrix extracted in this pass from linked assets. |
| https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Duschrinne_ShowerDrain_S-Plus.pdf | Official S+ brochure PDF | Profile+drain article tables | Official brochure source confirmed for S+ family. | Dedicated article-table normalization (article columns + explicit compatibility pairs) still incomplete. |
| https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Badentwaesserung_Linie.pdf | Official line brochure PDF | Technical parameter cross-check | Family-level technical comparison signals visible. | Not yet converted into per-article compatibility rows. |

---

## 3) Extracted profile/channel article table

> Result of dedicated pass: **no complete source-backed article list could be normalized yet** for S+ profile/channel rows.

| Component role | Article number | Product name | Length | Material | Sealing / W3-I | Source URL/PDF | Confidence | Notes |
|---|---|---|---|---|---|---|---|---|
| Profile/channel/base | Not conclusively extracted | ACO ShowerDrain S+ profile/channel family | Not normalized | Not normalized | W3-I context exists at family/doc level | S+ family page + brochures | Medium-Low | Requires explicit table extraction with article columns. |

---

## 4) Extracted drain body article table

| Component role | Article number | Product name | water_seal_mm | flow_rate_lps | flow_rate_10mm_lps | flow_rate_20mm_lps | outlet_dn | outlet_orientation | height range | DIN EN 1253 | Source URL/PDF | Confidence |
|---|---|---|---:|---:|---:|---:|---|---|---|---|---|---|
| Drain body (S+ compatible branch, WS50 evidence) | Not fully normalized | Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+ | 50 | 0.8 (from visible 20mm branch value) | 0.7 | 0.8 | DN50 | Horizontal/waagerecht context | Not fully extracted | DIN EN 1253-1 context present | S+ Ablaufkörper detail page | High (visible values), Medium (full-variant normalization) |
| Drain body (alternate branch evidence) | Not fully normalized | Ablaufkörper … ShowerDrain S+ | 30 (branch text visible) | Incomplete in current extracted snippet | Incomplete | Incomplete | DN50 context | Likely horizontal | Not fully extracted | DIN context present | S+ Ablaufkörper detail page | Medium |

---

## 5) Compatibility matrix (explicitly evidenced)

| Profile/channel article or family | Compatible drain-body article or family | Exact source wording (short excerpt) | Source URL/PDF | Confidence |
|---|---|---|---|---|
| ShowerDrain S+ family (profile side) | S+ Ablaufkörper family | “Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+” | S+ Ablaufkörper detail page | High |
| ShowerDrain S+ family | Profile + Ablauf modular relation | “Baukastensystem bestehend aus Duschrinnenprofil und Ablauf” | S+ family page | High |

**Still missing for safe assembled generation:** explicit article-to-article compatibility entries (e.g., `profile article X -> drain body article Y`).

---

## 6) Technical parameter evidence table (consolidated)

| Parameter | Explicit official evidence in this pass? | Best source | Confidence | Notes |
|---|---|---|---|---|
| flow_rate_lps | Partial | S+ Ablaufkörper detail page | Medium-High | Present for at least one visible branch; not fully normalized by article. |
| flow_rate_10mm_lps | Partial | S+ Ablaufkörper detail page | Medium | Visible for at least one branch; not complete across all variants. |
| flow_rate_20mm_lps | Partial | S+ Ablaufkörper detail page | Medium | Visible for at least one branch; not complete across all variants. |
| water_seal_mm | Partial-Strong | S+ Ablaufkörper detail page | High (branch-level), Medium (matrix-level) | WS values visible (e.g., 50/30 branches). |
| outlet_dn | Strong | S+ Ablaufkörper detail page | High | DN50 context explicit. |
| outlet_orientation | Partial | S+ Ablaufkörper detail page | Medium | Horizontal context visible. |
| height_adj_min_mm / max | Weak | Family/PDF context | Low | Not extracted in complete numeric article table. |
| material | Weak-Partial | Family/PDF context | Low-Medium | Not normalized by article. |
| sealing / W3-I | Partial | Family/PDF context | Medium | Mentioned but not article-mapped. |
| DIN EN 1253 | Partial-Strong | S+ Ablaufkörper detail page | Medium-High | Explicit DIN EN 1253-1 context. |

---

## 7) Missing evidence and blocker assessment

### Missing evidence
1. Full S+ **profile/channel article table** with stable article numbers.
2. Full S+ **drain body article table** normalized by article across all variants.
3. Explicit **profile-article ↔ drain-body-article** compatibility matrix.

### Does missing data block assembled-system creation?
**Yes (for safe production-level benchmark assembly).**

Without article-level compatibility mapping, assembled rows would risk unsafe relationships and unreliable technical attribution.

---

## 8) Recommendation
**Recommendation:** **Defer full assembled-system implementation**.

Suggested next step (parser-preparation only):
1. Run a dedicated extraction helper against official S+ PDFs/tables to capture article columns and compatibility mappings.
2. Normalize article numbers and deduplicate by official article keys.
3. Only then create source-backed assembled S+ rows.

---

## 9) Regression guardrails (must remain true in future implementation)
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

