# ACO ShowerDrain B / B-line source-evidence audit

## Conclusion

**Approved direct finished-set production evidence.** The checked-in ACO
international product page explicitly describes ShowerDrain B as an all-in-one,
ready-to-install set containing the channel, grating, and gully. Its order table
identifies eight finished-set article variants: four lengths without a sealing
sleeve (`9010.78.70`–`9010.78.73`) and the same four lengths with an attached
sealing sleeve (`3018172`–`3018175`).

The eight approved articles are promoted directly into `Products`, `Comparison`,
`Scoring_Field_Coverage`, and the explicit flow-head scenario sheets with
`product_family = showerdrain_b` and
`assembly_model = integral_all_in_one_set`.

This is **not** an explicit matrix linking a separately orderable body article
to a separately orderable grate article. The evidence therefore must not be
transformed into `base_x_grate` combinations. Each production row records the
finished-set article number while leaving `body_article_number` and
`grate_article_number` empty.

## Technical evidence preserved

All eight variants retain the shared source-backed values:

- water seal: 30 mm;
- outlet: DN50;
- fixed installation height: 80 mm (not represented as an adjustable min/max range);
- flow: 0.40 l/s at 10 mm head and 0.46 l/s at 20 mm head.

The unconditional `flow_rate_lps` field remains empty because the source gives
two condition-dependent values and does not designate either as a default. The
eight products contribute 16 rows to `Conditional_Technical_Values`. The
`Comparison_flow_head_10mm` and `Comparison_flow_head_20mm` sheets resolve the
corresponding values without creating an unsupported scalar default.

## Production and publication boundary

The direct promotion adds eight B-line rows to `Products` and `Comparison`,
bringing both sheets to 88 rows. It intentionally adds no B-line rows to
`BOM_Options`, `Final_Assemblies`, or `Final_Set_Details` because those sheets
represent generated component combinations or assemblies, while each B-line
article is already a complete integral finished set.

`Bline_Source_Evidence` remains the eight-row article-level evidence record. No
body × grate generation or compatibility inference is performed. Default
`ready_for_benchmark`, `ready_for_customer_view`, and customer publication
remain blocked pending approval of a conditional-flow policy; the explicit
scenario sheets are diagnostic resolutions and do not silently select a default.
