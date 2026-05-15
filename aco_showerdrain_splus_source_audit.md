# ACO ShowerDrain S+ Source Audit

## 1) Executive summary
**Can S+ be safely assembled now?** **Partial**.

**Reason:** Official ACO sources clearly show S+ is modular (profile + separate Ablaufkörper, plus accessories/grates), and at least one official S+ Ablaufkörper page includes explicit hydraulic values (10/20 mm flow, water seal, DN, DIN EN 1253-1 context). However, full article-level compatibility mapping (stable article numbers across all S+ subcomponents and explicit many-to-many compatibility table) is not fully confirmed from currently inspected official pages/snippets.

---

## 2) Official sources checked

| URL | Source type | Found | Not found |
|---|---|---|---|
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ | Product family page | “Baukastensystem bestehend aus Duschrinnenprofil und Ablauf”; separate entries for profile + Ablaufkörper; S+ family structure visible. | Complete normalized article table for all S+ components not confirmed from snippet alone. |
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/ | Product detail page (Ablaufkörper) | Explicit compatibility wording (“zu … ShowerDrain S+”), DN50, W3-I/DIN18534 statements, DIN EN 1253-1 class mention, explicit flow at 10/20 mm and water-seal values by variant text. | Full per-article matrix for every S+ profile/length combination not confirmed. |
| https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/ | Category page | S+ appears in official bathroom channel family scope. | No complete compatibility matrix. |
| https://www.aco-haustechnik.de/downloads/ | Download page | Official docs/PDF entry point exists. | Directly indexed S+ technical datasheet table-to-article mapping not fully confirmed in inspected snippets. |
| https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Duschrinne_ShowerDrain_S-Plus.pdf | Official PDF brochure | S+ official brochure exists. | Verified structured article compatibility table not confirmed from snippet. |
| https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Badentwaesserung_Linie.pdf | Official PDF line brochure | Comparative technical rows for S+ visible in snippet (flow, W3-I, profile characteristics). | Stable BOM-grade article compatibility mapping across components not confirmed. |

---

## 3) Component model

| Component role | Article number found? | Product name | Source URL | Confidence | Notes |
|---|---:|---|---|---|---|
| Profile/channel/base | Not confirmed in inspected snippets | ACO ShowerDrain S+, Duschrinnenprofil | S+ family page | Medium | Family page describes profile element and modular concept. |
| Drain body | Not confirmed in snippet for exact article no. | Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+ | Ablaufkörper detail page | High | Explicitly tied to S+ profile; strong role clarity. |
| Grate/cover | Family-level mention | Designrost / profile+rost context | S+ family page | Medium | Role is clear; exact article map not fully extracted. |
| Accessory | Family-level mention | ACO ShowerStep – Gefällekeil (compatible family mention incl. S+) | ShowerStep pages | Medium | Accessory relation is plausible, but direct S+ compatibility matrix granularity not fully proven. |

---

## 4) Compatibility evidence

| Base/profile family | Compatible component family | Exact wording / excerpt | Source URL | Confidence |
|---|---|---|---|---|
| ShowerDrain S+ profile | S+ Ablaufkörper | “Ablaufkörper zu ACO Duschrinnenprofil ShowerDrain S+” | Ablaufkörper detail page | High |
| ShowerDrain S+ family | Profile + Ablauf modular structure | “Baukastensystem bestehend aus Duschrinnenprofil und Ablauf” | S+ family page | High |
| ShowerDrain S+ family | Accessory (ShowerStep) | “optimale Ergänzung zu ACO ShowerDrain E+, M+, S+” | ShowerStep page(s) | Medium |

---

## 5) Technical parameter evidence

| Article/component | flow_rate_lps | flow_rate_10mm_lps | flow_rate_20mm_lps | water_seal_mm | outlet_dn | outlet_orientation | height range | Source URL | Confidence |
|---|---:|---:|---:|---:|---|---|---|---|---|
| S+ Ablaufkörper (WS50 branch text) | 0.8 (from 20 mm in snippet) | 0.7 | 0.8 | 50 | DN50 | Horizontal (waagerecht) | Not fully confirmed from snippet | Ablaufkörper detail page | High |
| S+ Ablaufkörper (WS30 branch text) | Mentioned but truncated in snippet | Not fully visible | Not fully visible | 30 (mentioned branch) | DN50 (page-level) | Horizontal | Not confirmed | Ablaufkörper detail page | Medium |
| S+ family page generic | Generic “Abflussvermögen” claims | No | No | No explicit numeric value on family snippet | No | No | No | S+ family page | Low |

---

## 6) Proposed benchmark model

### Current conclusion: **Not enough for full safe assembled rollout yet** (without deeper extraction pass of official PDFs/tables).

Missing/uncertain items that block fully safe assembly creation right now:
1. Stable article-number-level mapping for S+ profile/channel components.
2. Explicit compatibility matrix linking specific profile articles/lengths to specific drain-body articles.
3. Complete, machine-parseable per-variant technical table for all intended assembled combinations.

### Recommendation at this stage
- Do **not** create new benchmark assembled S+ product rows yet.
- Keep only conservative, source-backed BOM compatibility hints where explicit wording is present.
- Perform a dedicated PDF/table extraction pass on official S+ technical docs before assembly generation.

---

## 7) Implementation recommendation
**Defer due to insufficient official article-level compatibility evidence** for a full assembly model.

If implementation is attempted next:
1. Add parser support for official S+ technical PDF/article tables.
2. Require explicit article-to-article compatibility mapping before creating assembled_system rows.
3. Keep 10/20mm flow empty unless directly present for the chosen component/article pair.

---

## 8) Regression guardrails
Any future S+ implementation must preserve:
- Candidates_All >= 69
- Products >= 27
- Components >= 54
- Comparison >= 27
- BOM_Options >= 81 unless bad rows are intentionally removed
- Scoring_Field_Coverage >= 27
- ShowerDrain C monitored products remain in Products and Comparison
- ShowerDrain C water_seal_mm stays 25 / 25 / 50 / 50
- no non-ACO connector changes
- no scoring formula changes

