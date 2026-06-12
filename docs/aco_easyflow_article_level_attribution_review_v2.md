# ACO Easyflow article-level attribution review v2

## Scope and safety boundary

This is a diagnostic-only, fail-closed second-stage review of the two canonical
ACO Easyflow assemblies. It does not change canonical product generation,
assembly generation, scoring, workbook counts, article promotion, or benchmark
and customer-view readiness.

The production state remains:

- two Easyflow `Final_Assemblies` rows and two matching `Final_Set_Details` rows;
- `water_seal_mm = 50` and `outlet_dn = DN50`;
- empty `flow_rate_lps`, `height_adj_min_mm`, and `height_adj_max_mm`;
- `data_quality_status = partial`;
- `ready_for_benchmark = False`;
- `ready_for_customer_view = False`.

## Evidence conclusion

No checked-in record currently links either exact assembled product or selected
top component to one unique drain-body article and its technical values. The
shared WS50/DN50 tuple matches all three candidate articles. It therefore cannot
be used to select an article, flow value, or adjustable-height range.

Current result:

- Easyflow assemblies reviewed: **2**;
- candidate articles retained: **3**;
- explicit unique article matches: **0**;
- unique source-backed technical matches: **0**;
- blocked ambiguous assemblies: **2**;
- promotion-review-ready rows: **0**;
- overall: `article_level_attribution_not_resolved`.

## Candidate-article inventory

The diagnostic preserves each article-specific source record independently. It
does not merge values between rows or collapse conflicting values into a family
default.

| Article | Water seal | Outlet | Flow | Adjustable height | Side inlet / condition | Attribution status |
|---|---:|---|---:|---:|---|---|
| `2500.00.00` | 50 mm | DN50 | 1.5 l/s | 15–96 mm | retained exactly as recorded by the source row | candidate only |
| `2500.05.00` | 50 mm | DN50 | 1.0 l/s | 7–75 mm | retained exactly as recorded by the source row | candidate only |
| `2500.55.00` | 50 mm | DN50 | 1.5 l/s | 15–96 mm | retained exactly as recorded by the source row | candidate only |

The CLI prints the original article text, normalized article number, source URL,
source-record text, technical fields, evidence type, and confidence for every
available article-specific record. Empty evidence remains empty.

## Unresolved conflicts

The common assembly facts (`WS50`, `DN50`) do not distinguish the candidates.
The candidate records disagree on:

- `flow_rate_lps` (`1.0` versus `1.5`);
- `height_adj_min_mm` (`7` versus `15`);
- `height_adj_max_mm` (`75` versus `96`);
- any article-specific variant or side-inlet condition recorded by the source.

The selected Easyflow top component identifies the top component only. It does
not identify which drain body was used. Family-page co-occurrence, article
ordering, first-row selection, minimum/maximum selection, and combining values
from separate article rows are explicitly rejected.

## Attribution rules implemented

The diagnostic uses these states:

- `explicit_unique_article_match`;
- `unique_source_backed_technical_match`;
- `ambiguous_multiple_article_candidates`;
- `insufficient_article_level_evidence`;
- `conflicting_article_level_evidence`;
- `invalid_source_record`.

An explicit match requires a source URL, exact assembly or selected-component
identity, article number, and supporting technical evidence in the same record.
Article numbers embedded only in family-level prose are not explicit evidence.

A technical match is evaluated one source row at a time. Every populated
assembly tuple field must be supported and matched by that same source row.
Values from different rows cannot be combined to manufacture a unique tuple.
Only an explicit unique match can set the diagnostic
`ready_for_article_promotion_review = True`; even then, production changes
require a separate manual approval branch.

## Evidence required for promotion review

Obtain one primary-source ACO technical table, catalogue row, datasheet, or
product-page record that contains all of the following in the same record:

1. the exact drain-body or assembly identity used by one canonical Easyflow set;
2. the exact article number (`2500.00.00`, `2500.05.00`, or `2500.55.00`);
3. the supporting technical tuple, including WS, outlet DN, flow, installation
   or adjustment height, and any side-inlet or variant condition that applies;
4. enough information to exclude both competing article candidates.

Until that evidence is checked in and manually approved, both Easyflow rows
remain blocked and no flow or height value may be promoted.

## Read-only operation

Run either:

```bash
python tools/report_easyflow_article_level_attribution.py
python tools/report_easyflow_article_level_attribution.py --xlsx "path/to/workbook.xlsx"
```

For `--xlsx`, the tool hashes the workbook before and after reading it and exits
non-zero if the bytes differ. It never writes the workbook.
