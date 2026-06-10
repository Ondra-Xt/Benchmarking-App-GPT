# ACO ShowerDrain C+ source-evidence audit

## Decision

**Explicit article-level C+ base-to-grate compatibility was found.** The stored official ACO 2025 ShowerDrain catalog is an `explicit_catalog_matrix` with high confidence:

- Source fixture: `tests/fixtures/aco_cplus/cplus_showerdrain_catalog_2025_cz.pdf`
- Official source URL: <https://www.aco.cz/fileadmin/standard/aco.cz/04_Ke_stazeni/Ceniky/Dokumente/2025/ACO_ShowerDrain_katalog_2025_CZ.pdf>
- Page 25: `ACO ShowerDrain C+` body article table, including standard H=92 and low H=69 variants.
- Page 26: stainless grate article table explicitly headed `ACO ShowerDrain C & C+`.
- Compatibility rule used by the diagnostic: body and grate must have the same nominal length.

This finding first upgraded the source-backed rows in `Cplus_Compatible_Grate_Evidence` and now supports the canonical production baseline. The protected bases and 15 non-Tile grates generate 30 C+ assemblies. Manual customer-publication approval has been granted for exactly these validated assemblies, and their production customer-view flags are enabled.

## Exact protected-base mappings

The catalog contains two flange variants for each protected hydraulic base and nominal length. The current diagnostic grate candidates cover Wave, Quadrato, and Massive articles at 685, 785, 885, 985, and 1185 mm.

### `aco-showerdrain-cplus-standard-h92`

| Length | C+ base articles | Proven grate articles used by the diagnostic |
|---:|---|---|
| 685 mm | 9010.85.20, 9010.85.30 | 9010.88.61, 9010.88.68, 9010.88.89 |
| 785 mm | 9010.85.21, 9010.85.31 | 9010.88.62, 9010.88.69, 9010.88.90 |
| 885 mm | 9010.85.22, 9010.85.32 | 9010.88.63, 9010.88.70, 9010.88.91 |
| 985 mm | 9010.85.23, 9010.85.33 | 9010.88.64, 9010.88.71, 9010.88.92 |
| 1185 mm | 9010.85.24, 9010.85.34 | 9010.88.66, 9010.88.73, 9010.88.94 |

### `aco-showerdrain-cplus-low-h69`

| Length | C+ base articles | Proven grate articles used by the diagnostic |
|---:|---|---|
| 685 mm | 9010.85.40, 9010.85.50 | 9010.88.61, 9010.88.68, 9010.88.89 |
| 785 mm | 9010.85.41, 9010.85.51 | 9010.88.62, 9010.88.69, 9010.88.90 |
| 885 mm | 9010.85.42, 9010.85.52 | 9010.88.63, 9010.88.70, 9010.88.91 |
| 985 mm | 9010.85.43, 9010.85.53 | 9010.88.64, 9010.88.71, 9010.88.92 |
| 1185 mm | 9010.85.44, 9010.85.54 | 9010.88.66, 9010.88.73, 9010.88.94 |

The source catalog also proves length-matched Tile rows (9010.88.82-.85 and 9010.88.87), but they are not in the current `PLAUSIBLE_CPLUS_GRATE_ARTICLES` diagnostic candidate set and therefore are not introduced into the evidence export by this change.

## Sources inspected

| Source | Classification | Finding |
|---|---|---|
| `cplus_showerdrain_catalog_2025_cz.pdf` | `explicit_catalog_matrix` | Explicit C+ body articles on page 25 and C & C+ grate article table on page 26. |
| `cplus_bathroom_channels_catalog_cz.pdf` | `insufficient` | C+ and grate articles co-occur, but the parsed grate section is headed ShowerDrain C rather than an explicit C+ article matrix. |
| `cplus_family_cz.html` | `page_level_cplus_family` | Confirms the C+ family offers grate choices, but does not connect protected body articles to grate article numbers. |
| `c_design_grates_de.html` | `inferred_from_shared_c_grate_page` | Identifies ShowerDrain C grates only; it remains insufficient by itself. |
| `c_standard_h92_de.html`, `c_low_h69_de.html` | `insufficient` | C-family body pages; 9010.85.xx body articles are not treated as grate articles. |
| `c_family_cz.html`, `c_family_de.html`, `c_showerstep_de.html` | `insufficient` | Context only; no explicit C+ article-level compatibility matrix. |

## Protected hydraulics and readiness behavior

The protected values remain unchanged:

- Standard H=92: 0.91 l/s, 50 mm seal, DN50, adjustment 80-128 mm.
- Low H=69: 0.62 l/s, 25 mm seal, DN40, adjustment 57-128 mm.

For catalog-proven rows only:

- `compatibility_evidence_type = explicit_catalog_matrix`
- `compatibility_confidence = high`
- `article_level_compatibility_found = True`
- `safe_to_generate = True` as a diagnostic readiness signal
- `ready_for_benchmark = True` because protected technical fields are complete
- `ready_for_customer_view = False`

The reviewed production milestone promotes exactly the 30 catalogue-proven combinations into `BOM_Options`, `Final_Assemblies`, and `Final_Set_Details`: two protected bases multiplied by 15 Wave, Quadrato, and Massive grates. Tile articles remain excluded, protected hydraulic values remain unchanged, and `ready_for_customer_view` remains false.
