# ACO ShowerDrain B / B-line source-evidence audit

## Conclusion

**Classification A: valid diagnostic-only article evidence.** The checked-in ACO international product page explicitly describes ShowerDrain B as an all-in-one, ready-to-install set containing the channel, grating, and gully. Its order table identifies eight finished-set article variants: four lengths without a sealing sleeve (`9010.78.70`–`9010.78.73`) and the same four lengths with an attached sealing sleeve (`3018172`–`3018175`).

This is **not** an explicit matrix linking a separately orderable body article to a separately orderable grate article. The evidence therefore must not be transformed into `base_x_grate` combinations. The diagnostic records the finished-set article number while leaving separate body and grate article fields empty.

## Technical evidence preserved

All eight variants retain the shared source-backed values:

- water seal: 30 mm;
- outlet: DN50;
- fixed installation height: 80 mm (not represented as an adjustable min/max range);
- flow: 0.4 l/s at 10 mm build-up and 0.46 l/s at 20 mm build-up.

The unconditional `flow_rate_lps` field remains empty because the source gives two condition-dependent values and does not designate either as a default.

## Production gate

Every row in `Bline_Source_Evidence` is marked `safe_to_generate=false`, `ready_for_benchmark=false`, and `ready_for_customer_view=false`. This branch does not add B rows to Products, Comparison, BOM options, final assemblies, final-set details, scoring, scenarios, or customer-facing output.

A later dedicated branch may evaluate direct promotion of the eight explicit all-in-one articles as finished products. It must not infer separately selectable body/grate compatibility from this evidence.
