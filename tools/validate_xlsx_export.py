import argparse
import sys
from dataclasses import dataclass
from typing import Any, List, Tuple

import pandas as pd


# Configurable baseline expectations
EXPECTED_SHEET_COUNTS = {
    "Products": 80,
    "Comparison": 80,
    "Scoring_Field_Coverage": 80,
    "Candidates_All": 118,
    "Components": 100,
    "BOM_Options": 251,
    "Final_Assemblies": 62,
    "Final_Set_Details": 62,
    "Mplus_Compound_Mappings": 4,
    "Eplus_Proposal_Mappings": 3,
    "Conditional_Technical_Values": 8,
    "Article_Variants": 76,
    "Scoring_Scenarios": 3,
    "Comparison_flow_head_10mm": 80,
    "Comparison_flow_head_20mm": 80,
    "Cplus_Compatible_Grate_Evidence": 30,
}
COMPONENTS_MIN_ROWS = 1
EXPECTED_BOM_OPTION_TYPE_COUNTS = {
    "optional_accessory": 144,
    "compatible_grate": 83,
}
EXPECTED_ASSEMBLED_PREFIX_COUNTS = {
    "aco-assembled-showerdrain-splus": 16,
    "aco-assembled-showerdrain-c": 4,
    "aco-assembled-showerdrain-mplus": 4,
    "aco-assembled-showerdrain-eplus": 0,
    "aco-assembled-showerdrain-b": 0,
    "aco-assembled-showerdrain-cplus": 30,
}
EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS = {
    "easyflow": 2,
    "easyflowplus": 6,
    "showerdrain_c": 4,
    "showerdrain_splus": 16,
    "showerdrain_mplus": 4,
    "showerdrain_cplus": 30,
}
FINAL_ASSEMBLIES_PREFIX = "aco-assembled-"
FINAL_ASSEMBLIES_EASYFLOW_EXPECTED = {
    "water_seal_mm": 50,
    "outlet_dn": "DN50",
}
FINAL_ASSEMBLIES_EASYFLOW_EMPTY_FIELDS = [
    "flow_rate_lps",
    "height_adj_min_mm",
    "height_adj_max_mm",
]
FINAL_ASSEMBLIES_REQUIRED_COMPLETENESS_COLUMNS = [
    "is_complete_technical_data",
    "missing_technical_fields",
    "data_quality_status",
    "source_status_note",
]
EXPECTED_FINAL_ASSEMBLIES_STATUS_COUNTS = {
    "complete": 26,
    "explicit_source_ready_production_assembly": 30,
    "partial": 2,
    "conditional_parameter_available_production_blocked": 4,
    "missing": 0,
}
EXPECTED_FINAL_SET_DETAILS_ROW_COUNT = 62
EXPECTED_FINAL_SET_DETAILS_FAMILY_COUNTS = EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS.copy()
EXPECTED_FINAL_SET_DETAILS_READY_COUNTS = {
    True: 56,
    False: 6,
}
FINAL_SET_DETAILS_REQUIRED_COLUMNS = [
    "set_id",
    "assembled_product_id",
    "assembled_family",
    "manufacturer",
    "product_name",
    "base_product_id",
    "component_id",
    "component_role",
    "component_family",
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "is_complete_technical_data",
    "missing_technical_fields",
    "data_quality_status",
    "source_status_note",
    "ready_for_benchmark",
    "ready_for_customer_view",
    "blocked_reason",
    "article_variant_status",
    "article_variant_note",
    "product_url",
    "source_url",
    "sources",
]
FINAL_SET_DETAILS_EASYFLOW_BLOCKED_SNIPPET = "flow/height ambiguous"
FINAL_ASSEMBLIES_EASYFLOW_MISSING_FIELDS = (
    "flow_rate_lps,height_adj_min_mm,height_adj_max_mm"
)
FINAL_ASSEMBLIES_EASYFLOW_NOTE_SNIPPET = "ambiguous at current article/variant granularity"
FORBIDDEN_SYSTEM_ROLES = {"grate", "accessory", "optional_accessory"}
REQUIRED_SHEETS = [
    "Products",
    "Comparison",
    "Scoring_Field_Coverage",
    "Candidates_All",
    "Components",
    "BOM_Options",
    "Final_Assemblies",
    "Final_Set_Details",
    "Mplus_Compound_Mappings",
    "Eplus_Proposal_Mappings",
    "Cplus_Compatible_Grate_Evidence",
    "Conditional_Technical_Values",
    "Article_Variants",
    "Scoring_Scenarios",
    "Comparison_flow_head_10mm",
    "Comparison_flow_head_20mm",
]
OPTIONAL_SHEETS = ["Evidence"]

CPLUS_COMPATIBLE_GRATE_EVIDENCE_REQUIRED_COLUMNS = [
    "set_id", "product_family", "assembly_model", "base_id", "base_article_number",
    "base_source_url", "grate_id", "grate_article_number", "grate_source_url",
    "flow_rate_lps", "water_seal_mm", "outlet_dn", "height_adj_min_mm",
    "height_adj_max_mm", "compatibility_evidence_type", "compatibility_confidence",
    "article_level_compatibility_found", "source_text_or_reason", "data_quality_status",
    "safe_to_generate", "ready_for_benchmark", "ready_for_customer_view",
    "blocking_reason", "recommended_next_action", "production_status_note",
]
CPLUS_ALLOWED_BASE_IDS = {
    "aco-showerdrain-cplus-standard-h92",
    "aco-showerdrain-cplus-low-h69",
}
CPLUS_EXPLICIT_EVIDENCE_TYPES = {
    "article_level_explicit", "article_level_table", "explicit_catalog_matrix",
    "explicit_manual_matrix", "explicit_technical_drawing_matrix",
}
CPLUS_DIAGNOSTIC_PRODUCTION_NOTE_SNIPPET = "diagnostic/evidence-only"
CPLUS_GRATE_ARTICLE_RE = r"^9010\.88\.\d{2}$"


SCORING_SCENARIOS_REQUIRED_COLUMNS = [
    "scenario_id",
    "scenario_label",
    "parameter_name",
    "condition_type",
    "condition_value",
    "condition_unit",
    "policy",
    "is_default",
    "scoring_enabled",
    "customer_view_enabled",
    "notes",
]
REQUIRED_SCENARIO_IDS = {"no_scenario_selected", "flow_head_10mm", "flow_head_20mm"}
SCENARIO_COMPARISON_REQUIRED_COLUMNS = [
    "product_id",
    "manufacturer",
    "product_name",
    "product_family",
    "scenario_id",
    "flow_rate_lps",
    "flow_rate_resolution_status",
    "flow_rate_resolution_source",
    "flow_rate_condition_type",
    "flow_rate_condition_value",
    "flow_rate_condition_unit",
    "flow_rate_condition_label",
    "scenario_ready_for_benchmark",
    "scenario_blocked_reason",
    "scenario_scoring_note",
]
SCENARIO_SHEETS = {
    "Comparison_flow_head_10mm": ("flow_head_10mm", 0.40),
    "Comparison_flow_head_20mm": ("flow_head_20mm", 0.46),
}

MPLUS_COMPOUND_MAPPINGS_REQUIRED_COLUMNS = [
    "set_id",
    "product_family",
    "assembly_model",
    "channel_body_id",
    "channel_body_article_number",
    "drain_body_id",
    "drain_body_article_number",
    "grate_id",
    "grate_article_number",
    "source_url_channel_body",
    "source_url_drain_body",
    "source_url_grate",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "flow_rate_lps",
    "flow_rate_lps_10mm_head",
    "flow_rate_lps_20mm_head",
    "selected_default_flow_rate_lps",
    "accessory_flow_reduction_lps",
    "flow_policy",
    "flow_evidence_type",
    "flow_confidence",
    "flow_article_specific",
    "flow_attribution_scope",
    "missing_technical_fields",
    "data_quality_status",
    "safe_to_generate",
    "ready_for_benchmark",
    "ready_for_customer_view",
    "blocking_reason",
    "recommended_next_action",
    "production_status_note",
]
MPLUS_EXPECTED_ARTICLES = {"9010.81.20", "9010.81.21", "9010.81.22", "9010.81.23"}
MPLUS_BLOCKING_REASON_SNIPPET = "blocked_pending_conditional_parameter_scoring"

EPLUS_PROPOSAL_MAPPINGS_REQUIRED_COLUMNS = [
    "set_id",
    "product_family",
    "assembly_model",
    "body_id",
    "body_article_number",
    "body_source_url",
    "grate_id",
    "grate_article_number",
    "grate_source_url",
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "body_evidence_type",
    "body_confidence",
    "grate_evidence_type",
    "grate_confidence",
    "compatibility_evidence_type",
    "compatibility_confidence",
    "article_level_compatibility_found",
    "data_quality_status",
    "missing_evidence",
    "safe_to_generate",
    "ready_for_benchmark",
    "ready_for_customer_view",
    "blocking_reason",
    "recommended_next_action",
    "production_status_note",
]
EPLUS_EXPECTED_BODY_IDS = {
    "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm",
    "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm",
    "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1",
}
EPLUS_EXPECTED_GRATE_ID = "aco-showerdrain-eplus-design-roste-aus-elektropoliertem-edelstahl"
EPLUS_MISSING_EVIDENCE_SNIPPET = "explicit_article_level_base_to_grate_compatibility"
EPLUS_BLOCKING_REASON_SNIPPET = "no explicit article-level base-to-grate compatibility matrix"
EPLUS_RECOMMENDED_NEXT_ACTION_SNIPPET = (
    "collect explicit article-level E+ base-to-grate compatibility before production generation"
)
EPLUS_PRODUCTION_STATUS_NOTE_SNIPPET = (
    "diagnostic/proposal-only; no Products/BOM/assembly generation change"
)
MPLUS_PRODUCTION_STATUS_NOTE_SNIPPET = (
    "conditional flow values available in Conditional_Technical_Values; scenario scoring not implemented"
)

CONDITIONAL_TECHNICAL_VALUES_REQUIRED_COLUMNS = [
    "set_id",
    "product_family",
    "assembly_model",
    "parameter_name",
    "value",
    "unit",
    "condition_type",
    "condition_value",
    "condition_unit",
    "condition_label",
    "channel_body_id",
    "drain_body_id",
    "grate_id",
    "source_url_channel_body",
    "source_url_drain_body",
    "source_url_grate",
    "evidence_type",
    "confidence",
    "attribution_scope",
    "article_specific",
    "data_quality_status",
    "safe_to_generate",
    "ready_for_benchmark",
    "ready_for_customer_view",
    "blocking_reason",
    "recommended_next_action",
    "production_status_note",
]
CONDITIONAL_MPLUS_EXPECTED_ROW_COUNT = 8
CONDITIONAL_PRODUCTION_STATUS_NOTE_SNIPPET = "conditional flow values available in Conditional_Technical_Values; scenario scoring not implemented"
CONDITIONAL_RECOMMENDED_NEXT_ACTION_SNIPPET = "implement scoring/export handling for conditional parameter values before production M+ assemblies"

ARTICLE_VARIANTS_REQUIRED_COLUMNS = [
    "manufacturer",
    "base_product_id",
    "article_number",
    "variant_type",
    "product_family",
    "source_url",
    "water_seal_mm",
    "outlet_dn",
    "flow_rate_lps",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "cutout_mm",
    "side_inlet",
    "row_text",
    "attribution_status",
    "why_not_promoted",
]
EASYFLOW_WS50_DN50_EXPECTED_ARTICLES = {"2500.55.00", "2500.05.00", "2500.00.00"}
CPLUS_EXPECTED = {
    "aco-showerdrain-cplus-standard-h92": {
        "flow_rate_lps": 0.91,
        "water_seal_mm": 50,
        "outlet_dn": "DN50",
        "height_adj_min_mm": 80,
        "height_adj_max_mm": 128,
    },
    "aco-showerdrain-cplus-low-h69": {
        "flow_rate_lps": 0.62,
        "water_seal_mm": 25,
        "outlet_dn": "DN40",
        "height_adj_min_mm": 57,
        "height_adj_max_mm": 128,
    },
}
COMPATIBLE_GRATE_META_REQUIRED_SNIPPETS = [
    "compatibility_confidence=implicit_family_level",
    "explicit_article_matrix=false",
    "source_limitation=",
    "no explicit article-to-article matrix found",
]


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


APP_EXPORT_EXPECTED_COUNTS = {
    "Products": 80,
    "Comparison": 80,
    "Scoring_Field_Coverage": 80,
    "Candidates_All": 118,
    "Components": 100,
    "BOM_Options": 251,
    "Final_Assemblies": 62,
    "Final_Set_Details": 62,
    "Cplus_Compatible_Grate_Evidence": 30,
    "Comparison_flow_head_10mm": 80,
    "Comparison_flow_head_20mm": 80,
}

APP_EXPORT_EXPECTED_ASSEMBLED_COUNTS = {
    "aco-assembled-showerdrain-splus-": 16,
    "aco-assembled-showerdrain-c-": 4,
    "aco-assembled-showerdrain-mplus-": 4,
    "aco-assembled-showerdrain-cplus-": 30,
    "aco-assembled-showerdrain-eplus-": 0,
    "aco-assembled-showerdrain-b-": 0,
}


def validate_app_export_baseline(path: str) -> Tuple[bool, List[CheckResult]]:
    """Fast fail-closed preflight for a workbook exposed by Streamlit.

    This deliberately uses fixed canonical counts rather than the current UI/session
    membership.  A filtered connector run or stale partial session is therefore not
    eligible for download.
    """
    results: List[CheckResult] = []
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
    except Exception as exc:
        return False, [CheckResult("app_export_open", False, f"{type(exc).__name__}: {exc}")]

    required = list(APP_EXPORT_EXPECTED_COUNTS)
    missing = [name for name in required if name not in xls.sheet_names]
    results.append(CheckResult("app_export_required_sheets", not missing, f"missing={missing}"))
    if missing:
        xls.close()
        return False, results

    sheets = {name: pd.read_excel(xls, sheet_name=name) for name in required}
    xls.close()
    for name, expected in APP_EXPORT_EXPECTED_COUNTS.items():
        actual = len(sheets[name])
        results.append(CheckResult(
            f"app_export_row_count:{name}", actual == expected,
            f"actual={actual} expected={expected}",
        ))

    products = sheets["Products"]
    product_ids = _norm_series(products, "product_id").str.lower()
    for prefix, expected in APP_EXPORT_EXPECTED_ASSEMBLED_COUNTS.items():
        actual = int(product_ids.str.startswith(prefix).sum())
        results.append(CheckResult(
            f"app_export_assembled_count:{prefix}", actual == expected,
            f"actual={actual} expected={expected}",
        ))

    bom = sheets["BOM_Options"]
    cplus_bom = (
        _norm_series(bom, "option_type").str.lower().eq("compatible_grate")
        & _norm_series(bom, "product_family").str.lower().eq("showerdrain_cplus")
    )
    actual_cplus_bom = int(cplus_bom.sum())
    results.append(CheckResult(
        "app_export_cplus_bom_count", actual_cplus_bom == 30,
        f"actual={actual_cplus_bom} expected=30",
    ))
    return all(result.passed for result in results), results


def _norm_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    return df[col].fillna("").astype(str).str.strip()


def _compatible_grate_meta_valid(meta: str) -> bool:
    text = str(meta or "").lower()
    explicit = all(snippet in text for snippet in (
        "compatibility_confidence=high",
        "explicit_article_matrix=true",
        "equal_nominal_length=true",
    ))
    implicit = all(snippet in text for snippet in COMPATIBLE_GRATE_META_REQUIRED_SNIPPETS)
    return explicit or implicit


def _empty_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return df[col].isna() | df[col].astype(str).str.strip().eq("")


def _numeric_series_eq(df: pd.DataFrame, col: str, expected: float) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return pd.to_numeric(df[col], errors="coerce").eq(float(expected))


def _string_series_eq(df: pd.DataFrame, col: str, expected: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return df[col].fillna("").astype(str).str.strip().eq(expected)


def _coerce_bool_like(value: Any) -> bool | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    text = str(value).strip().lower()
    if text == "":
        return None
    if text in {"true", "1", "1.0", "yes", "y"}:
        return True
    if text in {"false", "0", "0.0", "no", "n"}:
        return False
    return None


def _bool_series_eq(df: pd.DataFrame, col: str, expected: bool) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    return pd.Series(
        [_coerce_bool_like(value) is expected for value in df[col]],
        index=df.index,
    )


def validate_xlsx(path: str) -> Tuple[bool, List[CheckResult]]:
    results: List[CheckResult] = []
    xls = pd.ExcelFile(path, engine="openpyxl")

    required_sheets = [s for s in REQUIRED_SHEETS if s in EXPECTED_SHEET_COUNTS or s in {"Scoring_Scenarios", *SCENARIO_SHEETS}]
    if "Final_Assemblies" in EXPECTED_SHEET_COUNTS and "Article_Variants" not in required_sheets:
        required_sheets.append("Article_Variants")
    if "Final_Assemblies" in EXPECTED_SHEET_COUNTS and "Final_Set_Details" not in required_sheets:
        required_sheets.append("Final_Set_Details")
    if "Cplus_Compatible_Grate_Evidence" not in required_sheets:
        required_sheets.append("Cplus_Compatible_Grate_Evidence")
    missing = [s for s in required_sheets if s not in xls.sheet_names]
    results.append(CheckResult("required_sheets", not missing, f"missing={missing} expected={required_sheets}"))
    if "Final_Assemblies" in required_sheets:
        final_sheet_exists = "Final_Assemblies" in xls.sheet_names
        results.append(CheckResult("final_assemblies_sheet_exists", final_sheet_exists, f"present={final_sheet_exists}"))
    if "Final_Set_Details" in required_sheets:
        detail_sheet_exists = "Final_Set_Details" in xls.sheet_names
        results.append(CheckResult("final_set_details_sheet_exists", detail_sheet_exists, f"present={detail_sheet_exists}"))
    if missing:
        xls.close()
        return False, results

    for s in OPTIONAL_SHEETS:
        results.append(CheckResult(f"optional_sheet:{s}", True, f"present={s in xls.sheet_names}"))

    sheets = {name: pd.read_excel(xls, sheet_name=name) for name in required_sheets}
    products, comparison, coverage, components, candidates, bom = (
        sheets["Products"],
        sheets["Comparison"],
        sheets["Scoring_Field_Coverage"],
        sheets["Components"],
        sheets["Candidates_All"],
        sheets["BOM_Options"],
    )
    final_assemblies = sheets.get("Final_Assemblies")
    final_set_details = sheets.get("Final_Set_Details")
    cplus_evidence = sheets.get("Cplus_Compatible_Grate_Evidence", pd.DataFrame())
    mplus_compound_mappings = (
        sheets.get("Mplus_Compound_Mappings")
        if "Mplus_Compound_Mappings" in sheets
        else (
            pd.read_excel(xls, sheet_name="Mplus_Compound_Mappings")
            if "Mplus_Compound_Mappings" in xls.sheet_names
            else pd.DataFrame(columns=MPLUS_COMPOUND_MAPPINGS_REQUIRED_COLUMNS)
        )
    )
    eplus_proposal_mappings = (
        sheets.get("Eplus_Proposal_Mappings")
        if "Eplus_Proposal_Mappings" in sheets
        else (
            pd.read_excel(xls, sheet_name="Eplus_Proposal_Mappings")
            if "Eplus_Proposal_Mappings" in xls.sheet_names
            else pd.DataFrame(columns=EPLUS_PROPOSAL_MAPPINGS_REQUIRED_COLUMNS)
        )
    )
    conditional_technical_values = (
        sheets.get("Conditional_Technical_Values")
        if "Conditional_Technical_Values" in sheets
        else (
            pd.read_excel(xls, sheet_name="Conditional_Technical_Values")
            if "Conditional_Technical_Values" in xls.sheet_names
            else pd.DataFrame(columns=CONDITIONAL_TECHNICAL_VALUES_REQUIRED_COLUMNS)
        )
    )
    article_variants = sheets.get("Article_Variants", pd.DataFrame(columns=ARTICLE_VARIANTS_REQUIRED_COLUMNS))

    scoring_scenarios = sheets["Scoring_Scenarios"]
    scenario_comparisons = {name: sheets[name] for name in SCENARIO_SHEETS}

    for name, expected in EXPECTED_SHEET_COUNTS.items():
        actual = len(sheets[name])
        results.append(CheckResult(f"row_count:{name}", actual == expected, f"actual={actual} expected={expected}"))

    cplus_missing_columns = [
        col for col in CPLUS_COMPATIBLE_GRATE_EVIDENCE_REQUIRED_COLUMNS if col not in cplus_evidence.columns
    ]
    results.append(CheckResult(
        "cplus_evidence_required_columns",
        not cplus_missing_columns,
        f"missing={cplus_missing_columns} expected={CPLUS_COMPATIBLE_GRATE_EVIDENCE_REQUIRED_COLUMNS}",
    ))
    if not cplus_missing_columns:
        set_ids = _norm_series(cplus_evidence, "set_id")
        base_ids = _norm_series(cplus_evidence, "base_id")
        evidence_types = _norm_series(cplus_evidence, "compatibility_evidence_type")
        articles = _norm_series(cplus_evidence, "grate_article_number")
        non_explicit = ~evidence_types.isin(CPLUS_EXPLICIT_EVIDENCE_TYPES)
        results.append(CheckResult("cplus_evidence_set_id_nonempty", set_ids.ne("").all(), f"empty={int(set_ids.eq('').sum())}"))
        results.append(CheckResult("cplus_evidence_set_id_no_url", ~set_ids.str.contains(r"https?://", case=False, regex=True).any(), "set_id must not contain URLs"))
        results.append(CheckResult("cplus_evidence_base_ids", base_ids.isin(CPLUS_ALLOWED_BASE_IDS).all(), f"actual={sorted(set(base_ids))}"))
        expected_bases_present = (set(base_ids) == CPLUS_ALLOWED_BASE_IDS) if CPLUS_EXPECTED else True
        results.append(CheckResult("cplus_evidence_two_protected_bases", expected_bases_present, f"actual={sorted(set(base_ids))} expected={sorted(CPLUS_ALLOWED_BASE_IDS)}"))
        grate_ids = _norm_series(cplus_evidence, "grate_id")
        self_reference = base_ids.eq(grate_ids) & grate_ids.ne("")
        results.append(CheckResult("cplus_evidence_no_self_reference", ~self_reference.any(), f"invalid_rows={list(cplus_evidence.index[self_reference])}"))
        family_ok = _norm_series(cplus_evidence, "product_family").eq("showerdrain_cplus")
        results.append(CheckResult("cplus_evidence_family_scope", family_ok.all(), f"invalid_rows={list(cplus_evidence.index[~family_ok])}"))
        plausible_articles = articles.eq("") | articles.str.match(CPLUS_GRATE_ARTICLE_RE)
        results.append(CheckResult("cplus_evidence_plausible_grate_articles", plausible_articles.all(), f"invalid={sorted(set(articles[~plausible_articles]))}"))
        hydraulic_ok = pd.Series(True, index=cplus_evidence.index)
        for base_id, expected in CPLUS_EXPECTED.items():
            rows = cplus_evidence[base_ids.eq(base_id)]
            row_ok = pd.Series(True, index=rows.index)
            for field, value in expected.items():
                if isinstance(value, str):
                    row_ok &= _string_series_eq(rows, field, value)
                else:
                    row_ok &= _numeric_series_eq(rows, field, value)
            hydraulic_ok.loc[rows.index] = row_ok
        results.append(CheckResult("cplus_evidence_protected_hydraulics", hydraulic_ok.all(), f"invalid_rows={list(cplus_evidence.index[~hydraulic_ok])}"))
        safe = pd.Series([_coerce_bool_like(v) for v in cplus_evidence["safe_to_generate"]], index=cplus_evidence.index)
        ready = pd.Series([_coerce_bool_like(v) for v in cplus_evidence["ready_for_benchmark"]], index=cplus_evidence.index)
        customer = pd.Series([_coerce_bool_like(v) for v in cplus_evidence["ready_for_customer_view"]], index=cplus_evidence.index)
        results.append(CheckResult("cplus_evidence_non_explicit_not_safe", (~non_explicit | safe.eq(False)).all(), f"invalid_rows={list(cplus_evidence.index[non_explicit & ~safe.eq(False)])}"))
        results.append(CheckResult("cplus_evidence_non_explicit_not_benchmark_ready", (~non_explicit | ready.eq(False)).all(), f"invalid_rows={list(cplus_evidence.index[non_explicit & ~ready.eq(False)])}"))
        results.append(CheckResult("cplus_evidence_customer_view_disabled", customer.eq(False).all(), f"invalid_rows={list(cplus_evidence.index[~customer.eq(False)])}"))
        notes = _norm_series(cplus_evidence, "production_status_note").str.lower()
        no_production = final_assemblies is None or not _norm_series(final_assemblies, "product_id").str.contains("cplus", case=False, regex=False).any()
        note_ok = notes.str.contains(CPLUS_DIAGNOSTIC_PRODUCTION_NOTE_SNIPPET, regex=False).all() if no_production else True
        results.append(CheckResult("cplus_evidence_diagnostic_only_note", bool(note_ok), f"no_production={no_production}"))
        final_ids = set(_norm_series(final_assemblies, "product_id")) if final_assemblies is not None else set()
        detail_set_ids = set(_norm_series(final_set_details, "set_id")) if final_set_details is not None else set()
        overlap = (set(set_ids) & final_ids) | (set(set_ids) & detail_set_ids)
        results.append(CheckResult("cplus_evidence_no_production_overlap", not overlap, f"overlap={sorted(overlap)}"))

    scenario_columns_missing = [col for col in SCORING_SCENARIOS_REQUIRED_COLUMNS if col not in scoring_scenarios.columns]
    results.append(CheckResult(
        "scoring_scenarios_required_columns",
        not scenario_columns_missing,
        f"missing={scenario_columns_missing} expected={SCORING_SCENARIOS_REQUIRED_COLUMNS}",
    ))
    scenario_ids = set(_norm_series(scoring_scenarios, "scenario_id"))
    results.append(CheckResult(
        "scoring_scenarios_required_ids",
        REQUIRED_SCENARIO_IDS.issubset(scenario_ids),
        f"actual={sorted(scenario_ids)} expected={sorted(REQUIRED_SCENARIO_IDS)}",
    ))
    default_rows = scoring_scenarios[_bool_series_eq(scoring_scenarios, "is_default", True)]
    default_ids = set(_norm_series(default_rows, "scenario_id"))
    results.append(CheckResult(
        "scoring_scenarios_safe_default",
        default_ids == {"no_scenario_selected"},
        f"actual_default_ids={sorted(default_ids)} expected=['no_scenario_selected']",
    ))

    comparison_ids = set(_norm_series(comparison, "product_id"))
    for sheet_name, (scenario_id, expected_flow) in SCENARIO_SHEETS.items():
        scenario_df = scenario_comparisons[sheet_name]
        missing_columns = [col for col in SCENARIO_COMPARISON_REQUIRED_COLUMNS if col not in scenario_df.columns]
        results.append(CheckResult(
            f"scenario_comparison_required_columns:{sheet_name}",
            not missing_columns,
            f"missing={missing_columns} expected={SCENARIO_COMPARISON_REQUIRED_COLUMNS}",
        ))
        results.append(CheckResult(
            f"scenario_comparison_row_count:{sheet_name}",
            len(scenario_df) == len(comparison),
            f"actual={len(scenario_df)} expected={len(comparison)}",
        ))
        scenario_ids_actual = set(_norm_series(scenario_df, "scenario_id"))
        results.append(CheckResult(
            f"scenario_comparison_scenario_id:{sheet_name}",
            scenario_ids_actual == {scenario_id},
            f"actual={sorted(scenario_ids_actual)} expected={[scenario_id]}",
        ))
        scenario_product_ids = set(_norm_series(scenario_df, "product_id"))
        results.append(CheckResult(
            f"scenario_comparison_product_membership:{sheet_name}",
            scenario_product_ids == comparison_ids,
            f"missing={sorted(comparison_ids - scenario_product_ids)} extra={sorted(scenario_product_ids - comparison_ids)}",
        ))
        mplus_scenario = scenario_df[
            _norm_series(scenario_df, "product_family").str.lower().eq("showerdrain_mplus")
        ]
        resolved_flow = pd.to_numeric(mplus_scenario.get("flow_rate_lps"), errors="coerce")
        expected_mplus_rows = EXPECTED_ASSEMBLED_PREFIX_COUNTS.get("aco-assembled-showerdrain-mplus", 0)
        flow_ok = len(mplus_scenario) == expected_mplus_rows and resolved_flow.notna().all() and resolved_flow.round(2).eq(expected_flow).all()
        results.append(CheckResult(
            f"scenario_mplus_flow:{sheet_name}",
            flow_ok,
            f"rows={len(mplus_scenario)} actual={resolved_flow.tolist()} expected={[expected_flow] * expected_mplus_rows}",
        ))
        source_bad = int((~_string_series_eq(mplus_scenario, "flow_rate_resolution_source", "Conditional_Technical_Values")).sum())
        status_bad = int((~_string_series_eq(mplus_scenario, "flow_rate_resolution_status", "resolved_from_condition")).sum())
        ready_bad = int((~_bool_series_eq(mplus_scenario, "scenario_ready_for_benchmark", True)).sum())
        results.append(CheckResult(f"scenario_mplus_resolution_source:{sheet_name}", source_bad == 0, f"actual_bad={source_bad} expected=Conditional_Technical_Values"))
        results.append(CheckResult(f"scenario_mplus_resolution_status:{sheet_name}", status_bad == 0, f"actual_bad={status_bad} expected=resolved_from_condition"))
        results.append(CheckResult(f"scenario_mplus_ready:{sheet_name}", ready_bad == 0, f"actual_bad={ready_bad} expected=true"))

        expected_cplus_rows = EXPECTED_ASSEMBLED_PREFIX_COUNTS.get("aco-assembled-showerdrain-cplus", 0)
        if expected_cplus_rows:
            cplus_scenario = scenario_df[
                _norm_series(scenario_df, "product_id").str.startswith("aco-assembled-showerdrain-cplus-")
            ]
            cplus_source_bad = int((~_string_series_eq(cplus_scenario, "flow_rate_resolution_source", "scalar_unconditional")).sum())
            cplus_status_bad = int((~_string_series_eq(cplus_scenario, "flow_rate_resolution_status", "resolved_from_scalar")).sum())
            cplus_ready_bad = int((~_bool_series_eq(cplus_scenario, "scenario_ready_for_benchmark", True)).sum())
            cplus_blocked_filled = int((~_empty_series(cplus_scenario, "scenario_blocked_reason")).sum())
            results.append(CheckResult(f"scenario_cplus_count:{sheet_name}", len(cplus_scenario) == expected_cplus_rows, f"actual={len(cplus_scenario)} expected={expected_cplus_rows}"))
            results.append(CheckResult(f"scenario_cplus_resolution_source:{sheet_name}", cplus_source_bad == 0, f"actual_bad={cplus_source_bad} expected=scalar_unconditional"))
            results.append(CheckResult(f"scenario_cplus_resolution_status:{sheet_name}", cplus_status_bad == 0, f"actual_bad={cplus_status_bad} expected=resolved_from_scalar"))
            results.append(CheckResult(f"scenario_cplus_ready:{sheet_name}", cplus_ready_bad == 0, f"actual_bad={cplus_ready_bad} expected=true"))
            results.append(CheckResult(f"scenario_cplus_blocked_reason_empty:{sheet_name}", cplus_blocked_filled == 0, f"actual_filled={cplus_blocked_filled} expected=0"))

    for default_sheet_name, default_df in (("Products", products), ("Comparison", comparison)):
        default_family = _norm_series(default_df, "product_family").str.lower()
        default_ids = _norm_series(default_df, "product_id").str.lower()
        default_mplus = default_df[
            default_family.eq("showerdrain_mplus")
            | default_ids.str.startswith("aco-assembled-showerdrain-mplus-")
        ]
        expected_mplus_rows = EXPECTED_ASSEMBLED_PREFIX_COUNTS.get("aco-assembled-showerdrain-mplus", 0)
        results.append(CheckResult(
            f"default_mplus_row_count:{default_sheet_name}",
            len(default_mplus) == expected_mplus_rows,
            f"actual={len(default_mplus)} expected={expected_mplus_rows}",
        ))
        default_flow_filled = int(_norm_series(default_mplus, "flow_rate_lps").ne("").sum())
        default_benchmark_bad = int((~_bool_series_eq(default_mplus, "ready_for_benchmark", False)).sum())
        default_customer_bad = int((~_bool_series_eq(default_mplus, "ready_for_customer_view", False)).sum())
        default_block_bad = int((~_norm_series(default_mplus, "blocked_reason").str.contains(MPLUS_BLOCKING_REASON_SNIPPET, regex=False)).sum())
        results.append(CheckResult(f"default_mplus_flow_empty:{default_sheet_name}", default_flow_filled == 0, f"actual_filled={default_flow_filled} expected=0"))
        results.append(CheckResult(f"default_mplus_ready_for_benchmark_false:{default_sheet_name}", default_benchmark_bad == 0, f"actual_bad={default_benchmark_bad} expected=false"))
        results.append(CheckResult(f"default_mplus_ready_for_customer_view_false:{default_sheet_name}", default_customer_bad == 0, f"actual_bad={default_customer_bad} expected=false"))
        results.append(CheckResult(f"default_mplus_blocked_reason:{default_sheet_name}", default_block_bad == 0, f"actual_bad={default_block_bad} expected_snippet={MPLUS_BLOCKING_REASON_SNIPPET}"))

    comp_ok = len(components) >= COMPONENTS_MIN_ROWS
    results.append(CheckResult("components_min_rows", comp_ok, f"actual={len(components)} expected>={COMPONENTS_MIN_ROWS}"))


    if "Mplus_Compound_Mappings" in required_sheets or "Mplus_Compound_Mappings" in xls.sheet_names:
        mplus_missing_columns = [
            col for col in MPLUS_COMPOUND_MAPPINGS_REQUIRED_COLUMNS
            if col not in mplus_compound_mappings.columns
        ]
        results.append(CheckResult(
            "mplus_compound_mappings_required_columns",
            not mplus_missing_columns,
            f"missing={mplus_missing_columns} expected={MPLUS_COMPOUND_MAPPINGS_REQUIRED_COLUMNS}",
        ))
        mplus_set_ids = _norm_series(mplus_compound_mappings, "set_id")
        mplus_set_ids_empty = int(mplus_set_ids.eq("").sum())
        mplus_set_ids_with_url = int(mplus_set_ids.str.lower().str.contains("http", regex=False).sum())
        results.append(CheckResult("mplus_compound_mappings_set_id_non_empty", mplus_set_ids_empty == 0, f"actual_empty={mplus_set_ids_empty} expected_empty=0"))
        results.append(CheckResult("mplus_compound_mappings_set_id_no_url", mplus_set_ids_with_url == 0, f"actual_with_url={mplus_set_ids_with_url} expected=0"))

        mplus_articles = set(_norm_series(mplus_compound_mappings, "drain_body_article_number"))
        missing_mplus_articles = sorted(MPLUS_EXPECTED_ARTICLES - mplus_articles)
        extra_mplus_articles = sorted(mplus_articles - MPLUS_EXPECTED_ARTICLES - {""})
        results.append(CheckResult(
            "mplus_compound_mappings_expected_articles",
            not missing_mplus_articles and not extra_mplus_articles,
            f"missing={missing_mplus_articles} extra={extra_mplus_articles} expected={sorted(MPLUS_EXPECTED_ARTICLES)}",
        ))

        mplus_string_expectations = {
            "product_family": "showerdrain_mplus",
            "assembly_model": "channel_body_x_drain_body_x_grate",
            "flow_policy": "split_fields_only",
            "data_quality_status": "partial",
        }
        for column, expected in mplus_string_expectations.items():
            bad = int((~_string_series_eq(mplus_compound_mappings, column, expected)).sum())
            results.append(CheckResult(f"mplus_compound_mappings_value:{column}", bad == 0, f"actual_bad={bad} expected={expected}"))

        mplus_numeric_expectations = {
            "flow_rate_lps_10mm_head": 0.4,
            "flow_rate_lps_20mm_head": 0.46,
            "accessory_flow_reduction_lps": 0.1,
        }
        for column, expected in mplus_numeric_expectations.items():
            bad = int((~_numeric_series_eq(mplus_compound_mappings, column, expected)).sum())
            results.append(CheckResult(f"mplus_compound_mappings_value:{column}", bad == 0, f"actual_bad={bad} expected={expected}"))

        for column in ["flow_rate_lps", "selected_default_flow_rate_lps"]:
            filled = int((~_empty_series(mplus_compound_mappings, column)).sum())
            results.append(CheckResult(f"mplus_compound_mappings_empty:{column}", filled == 0, f"actual_filled={filled} expected_filled=0"))

        for column in ["flow_article_specific", "safe_to_generate", "ready_for_benchmark", "ready_for_customer_view"]:
            bad = int((~_bool_series_eq(mplus_compound_mappings, column, False)).sum())
            results.append(CheckResult(f"mplus_compound_mappings_false:{column}", bad == 0, f"actual_bad={bad} expected=false"))

        missing_field_bad = int((~_norm_series(mplus_compound_mappings, "missing_technical_fields").str.contains("flow_rate_lps", regex=False)).sum())
        results.append(CheckResult("mplus_compound_mappings_missing_fields_contains_flow", missing_field_bad == 0, f"actual_bad={missing_field_bad} expected_contains=flow_rate_lps"))
        blocking_bad = int((~_norm_series(mplus_compound_mappings, "blocking_reason").str.lower().str.contains(MPLUS_BLOCKING_REASON_SNIPPET, regex=False)).sum())
        results.append(CheckResult("mplus_compound_mappings_blocking_reason", blocking_bad == 0, f"actual_bad={blocking_bad} expected_snippet={MPLUS_BLOCKING_REASON_SNIPPET}"))

        mplus_diagnostic_product_ids = sorted(set(mplus_set_ids) & set(_norm_series(products, "product_id")))
        expected_mplus_promoted = EXPECTED_ASSEMBLED_PREFIX_COUNTS.get("aco-assembled-showerdrain-mplus", 0)
        results.append(CheckResult(
            "mplus_compound_mappings_promoted_to_products",
            len(mplus_diagnostic_product_ids) == expected_mplus_promoted,
            f"overlap={mplus_diagnostic_product_ids} expected_count={expected_mplus_promoted}",
        ))


    if "Eplus_Proposal_Mappings" in required_sheets or "Eplus_Proposal_Mappings" in xls.sheet_names:
        eplus_missing_columns = [
            col for col in EPLUS_PROPOSAL_MAPPINGS_REQUIRED_COLUMNS
            if col not in eplus_proposal_mappings.columns
        ]
        results.append(CheckResult(
            "eplus_proposal_mappings_required_columns",
            not eplus_missing_columns,
            f"missing={eplus_missing_columns} expected={EPLUS_PROPOSAL_MAPPINGS_REQUIRED_COLUMNS}",
        ))

        eplus_set_ids = _norm_series(eplus_proposal_mappings, "set_id")
        eplus_set_ids_empty = int(eplus_set_ids.eq("").sum())
        eplus_set_ids_with_url = int(eplus_set_ids.str.lower().str.contains("http", regex=False).sum())
        results.append(CheckResult("eplus_proposal_mappings_set_id_non_empty", eplus_set_ids_empty == 0, f"actual_empty={eplus_set_ids_empty} expected_empty=0"))
        results.append(CheckResult("eplus_proposal_mappings_set_id_no_url", eplus_set_ids_with_url == 0, f"actual_with_url={eplus_set_ids_with_url} expected=0"))

        eplus_body_ids = set(_norm_series(eplus_proposal_mappings, "body_id"))
        eplus_body_ids_lower = {value.lower() for value in eplus_body_ids}
        expected_body_ids_lower = {value.lower() for value in EPLUS_EXPECTED_BODY_IDS}
        missing_body_ids = sorted(expected_body_ids_lower - eplus_body_ids_lower)
        extra_body_ids = sorted(eplus_body_ids_lower - expected_body_ids_lower - {""})
        results.append(CheckResult(
            "eplus_proposal_mappings_expected_body_ids",
            not missing_body_ids and not extra_body_ids,
            f"missing={missing_body_ids} extra={extra_body_ids} expected={sorted(EPLUS_EXPECTED_BODY_IDS)}",
        ))

        eplus_grate_ids = set(_norm_series(eplus_proposal_mappings, "grate_id"))
        eplus_grate_ids_lower = {value.lower() for value in eplus_grate_ids}
        expected_grate_id_lower = EPLUS_EXPECTED_GRATE_ID.lower()
        missing_grate = expected_grate_id_lower not in eplus_grate_ids_lower
        extra_grates = sorted(eplus_grate_ids_lower - {expected_grate_id_lower, ""})
        results.append(CheckResult(
            "eplus_proposal_mappings_expected_grate_id",
            not missing_grate and not extra_grates,
            f"missing={missing_grate} extra={extra_grates} expected={EPLUS_EXPECTED_GRATE_ID}",
        ))

        eplus_string_expectations = {
            "product_family": "showerdrain_eplus",
            "assembly_model": "base_x_grate",
            "outlet_dn": "DN50",
            "body_evidence_type": "source_page_level_body_url",
            "body_confidence": "high",
            "grate_evidence_type": "source_page_level_grate_url",
            "grate_confidence": "high",
            "compatibility_evidence_type": "page_level_family_bom_or_inferred_from_current_bom",
            "compatibility_confidence": "medium",
            "data_quality_status": "proposal_only_partial",
        }
        for column, expected in eplus_string_expectations.items():
            bad = int((~_string_series_eq(eplus_proposal_mappings, column, expected)).sum())
            results.append(CheckResult(f"eplus_proposal_mappings_value:{column}", bad == 0, f"actual_bad={bad} expected={expected}"))

        eplus_numeric_expectations = {
            "flow_rate_lps": 0.70,
            "water_seal_mm": 50,
            "height_adj_max_mm": 128,
        }
        for column, expected in eplus_numeric_expectations.items():
            bad = int((~_numeric_series_eq(eplus_proposal_mappings, column, expected)).sum())
            results.append(CheckResult(f"eplus_proposal_mappings_value:{column}", bad == 0, f"actual_bad={bad} expected={expected}"))

        height_source = eplus_proposal_mappings.get("height_adj_min_mm", pd.Series([], dtype=object))
        height_values = sorted(set(pd.to_numeric(height_source, errors="coerce").dropna().astype(int)))
        if not height_values:
            height_values = sorted({
                expected
                for body_id, expected in [
                    ("aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm", 25),
                    ("aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm", 57),
                    ("aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1", 80),
                ]
                if body_id in eplus_body_ids_lower
            })
        results.append(CheckResult("eplus_proposal_mappings_height_adj_min_mm", height_values == [25, 57, 80], f"actual={height_values} expected={[25, 57, 80]}"))

        for column in ["body_article_number", "grate_article_number"]:
            filled = int((~_empty_series(eplus_proposal_mappings, column)).sum())
            results.append(CheckResult(f"eplus_proposal_mappings_empty:{column}", filled == 0, f"actual_filled={filled} expected_filled=0"))

        for column in ["article_level_compatibility_found", "safe_to_generate", "ready_for_benchmark", "ready_for_customer_view"]:
            bad = int((~_bool_series_eq(eplus_proposal_mappings, column, False)).sum())
            results.append(CheckResult(f"eplus_proposal_mappings_false:{column}", bad == 0, f"actual_bad={bad} expected=false"))

        missing_evidence_bad = int((~_norm_series(eplus_proposal_mappings, "missing_evidence").str.contains(EPLUS_MISSING_EVIDENCE_SNIPPET, regex=False)).sum())
        results.append(CheckResult("eplus_proposal_mappings_missing_evidence", missing_evidence_bad == 0, f"actual_bad={missing_evidence_bad} expected_contains={EPLUS_MISSING_EVIDENCE_SNIPPET}"))
        blocking_bad = int((~_norm_series(eplus_proposal_mappings, "blocking_reason").str.lower().str.contains(EPLUS_BLOCKING_REASON_SNIPPET, regex=False)).sum())
        results.append(CheckResult("eplus_proposal_mappings_blocking_reason", blocking_bad == 0, f"actual_bad={blocking_bad} expected_snippet={EPLUS_BLOCKING_REASON_SNIPPET}"))
        next_action_bad = int((~_norm_series(eplus_proposal_mappings, "recommended_next_action").str.contains(EPLUS_RECOMMENDED_NEXT_ACTION_SNIPPET, regex=False)).sum())
        results.append(CheckResult("eplus_proposal_mappings_recommended_next_action", next_action_bad == 0, f"actual_bad={next_action_bad} expected_snippet={EPLUS_RECOMMENDED_NEXT_ACTION_SNIPPET}"))
        status_note_bad = int((~_norm_series(eplus_proposal_mappings, "production_status_note").str.contains(EPLUS_PRODUCTION_STATUS_NOTE_SNIPPET, regex=False)).sum())
        results.append(CheckResult("eplus_proposal_mappings_production_status_note", status_note_bad == 0, f"actual_bad={status_note_bad} expected_snippet={EPLUS_PRODUCTION_STATUS_NOTE_SNIPPET}"))

        proposed_ids = set(eplus_set_ids) - {""}
        eplus_product_ids = sorted(proposed_ids & set(_norm_series(products, "product_id")))
        eplus_final_ids = sorted(proposed_ids & set(_norm_series(final_assemblies, "product_id"))) if final_assemblies is not None else []
        eplus_detail_ids = sorted(proposed_ids & (set(_norm_series(final_set_details, "set_id")) | set(_norm_series(final_set_details, "assembled_product_id")))) if final_set_details is not None else []
        results.append(CheckResult("eplus_proposal_mappings_not_in_products", not eplus_product_ids, f"overlap={eplus_product_ids}"))
        results.append(CheckResult("eplus_proposal_mappings_not_in_final_assemblies", not eplus_final_ids, f"overlap={eplus_final_ids}"))
        results.append(CheckResult("eplus_proposal_mappings_not_in_final_set_details", not eplus_detail_ids, f"overlap={eplus_detail_ids}"))

    if "Conditional_Technical_Values" in required_sheets or "Conditional_Technical_Values" in xls.sheet_names:
        conditional_missing_columns = [
            col for col in CONDITIONAL_TECHNICAL_VALUES_REQUIRED_COLUMNS
            if col not in conditional_technical_values.columns
        ]
        results.append(CheckResult(
            "conditional_technical_values_required_columns",
            not conditional_missing_columns,
            f"missing={conditional_missing_columns} expected={CONDITIONAL_TECHNICAL_VALUES_REQUIRED_COLUMNS}",
        ))

        mplus_conditional = conditional_technical_values[
            _norm_series(conditional_technical_values, "product_family").str.lower().eq("showerdrain_mplus")
        ]
        results.append(CheckResult(
            "conditional_technical_values_mplus_row_count",
            len(mplus_conditional) == CONDITIONAL_MPLUS_EXPECTED_ROW_COUNT,
            f"actual={len(mplus_conditional)} expected={CONDITIONAL_MPLUS_EXPECTED_ROW_COUNT}",
        ))

        expected_variants = {(10, 0.4), (20, 0.46)}
        bad_set_ids: list[str] = []
        for set_id, rows in mplus_conditional.groupby(_norm_series(mplus_conditional, "set_id")):
            actual_variants = set(
                zip(
                    pd.to_numeric(rows.get("condition_value"), errors="coerce").dropna().astype(int),
                    pd.to_numeric(rows.get("value"), errors="coerce").dropna().round(2),
                )
            )
            if len(rows) != 2 or actual_variants != expected_variants:
                bad_set_ids.append(str(set_id))
        results.append(CheckResult(
            "conditional_technical_values_two_flow_rows_per_mplus_set",
            not bad_set_ids and mplus_conditional["set_id"].nunique() == 4,
            f"bad_set_ids={bad_set_ids} actual_set_ids={mplus_conditional['set_id'].nunique()} expected_set_ids=4",
        ))

        mplus_conditional_string_expectations = {
            "parameter_name": "flow_rate_lps",
            "unit": "l/s",
            "condition_type": "head_water_level",
            "condition_unit": "mm",
            "blocking_reason": MPLUS_BLOCKING_REASON_SNIPPET,
        }
        for column, expected in mplus_conditional_string_expectations.items():
            bad = int((~_string_series_eq(mplus_conditional, column, expected)).sum())
            results.append(CheckResult(f"conditional_technical_values_value:{column}", bad == 0, f"actual_bad={bad} expected={expected}"))

        labels = set(_norm_series(mplus_conditional, "condition_label"))
        results.append(CheckResult(
            "conditional_technical_values_condition_labels",
            labels == {"10 mm head water level", "20 mm head water level"},
            f"actual={sorted(labels)} expected={["10 mm head water level", "20 mm head water level"]}",
        ))

        for column in ["article_specific", "safe_to_generate", "ready_for_benchmark", "ready_for_customer_view"]:
            bad = int((~_bool_series_eq(mplus_conditional, column, False)).sum())
            results.append(CheckResult(f"conditional_technical_values_false:{column}", bad == 0, f"actual_bad={bad} expected=false"))

        status_note_bad = int((~_norm_series(mplus_conditional, "production_status_note").str.contains(CONDITIONAL_PRODUCTION_STATUS_NOTE_SNIPPET, regex=False)).sum())
        results.append(CheckResult("conditional_technical_values_production_status_note", status_note_bad == 0, f"actual_bad={status_note_bad} expected_snippet={CONDITIONAL_PRODUCTION_STATUS_NOTE_SNIPPET}"))
        next_action_bad = int((~_norm_series(mplus_conditional, "recommended_next_action").str.contains(CONDITIONAL_RECOMMENDED_NEXT_ACTION_SNIPPET, regex=False)).sum())
        results.append(CheckResult("conditional_technical_values_recommended_next_action", next_action_bad == 0, f"actual_bad={next_action_bad} expected_snippet={CONDITIONAL_RECOMMENDED_NEXT_ACTION_SNIPPET}"))

        conditional_set_ids = set(_norm_series(mplus_conditional, "set_id")) - {""}
        product_overlap = sorted(conditional_set_ids & set(_norm_series(products, "product_id")))
        final_overlap = sorted(conditional_set_ids & set(_norm_series(final_assemblies, "product_id"))) if final_assemblies is not None else []
        detail_overlap = sorted(conditional_set_ids & (set(_norm_series(final_set_details, "set_id")) | set(_norm_series(final_set_details, "assembled_product_id")))) if final_set_details is not None else []
        expected_mplus_promoted = EXPECTED_ASSEMBLED_PREFIX_COUNTS.get("aco-assembled-showerdrain-mplus", 0)
        results.append(CheckResult("conditional_technical_values_linked_to_mplus_products", len(product_overlap) == expected_mplus_promoted, f"overlap={product_overlap} expected_count={expected_mplus_promoted}"))
        results.append(CheckResult("conditional_technical_values_linked_to_mplus_final_assemblies", len(final_overlap) == expected_mplus_promoted, f"overlap={final_overlap} expected_count={expected_mplus_promoted}"))
        results.append(CheckResult("conditional_technical_values_linked_to_mplus_final_set_details", len(detail_overlap) == expected_mplus_promoted, f"overlap={detail_overlap} expected_count={expected_mplus_promoted}"))


    if "Article_Variants" in required_sheets or "Article_Variants" in xls.sheet_names:
        article_missing_columns = [col for col in ARTICLE_VARIANTS_REQUIRED_COLUMNS if col not in article_variants.columns]
        results.append(CheckResult(
            "article_variants_required_columns",
            not article_missing_columns,
            f"missing={article_missing_columns} expected={ARTICLE_VARIANTS_REQUIRED_COLUMNS}",
        ))
        article_source_blank = int(_norm_series(article_variants, "source_url").eq("").sum())
        results.append(CheckResult("article_variants_source_url_non_empty", article_source_blank == 0, f"actual_blank={article_source_blank} expected=0"))
        candidate_body = article_variants[_norm_series(article_variants, "variant_type").eq("candidate_body_variant")]
        candidate_article_blank = int(_norm_series(candidate_body, "article_number").eq("").sum())
        results.append(CheckResult("article_variants_candidate_article_number_non_empty", candidate_article_blank == 0, f"actual_blank={candidate_article_blank} expected=0"))
        product_ids = set(_norm_series(products, "product_id"))
        variant_article_ids = set(value for value in _norm_series(article_variants, "article_number") if value)
        promoted_article_ids = sorted(variant_article_ids & product_ids)
        results.append(CheckResult("article_variants_not_promoted_to_products", not promoted_article_ids, f"promoted={promoted_article_ids}"))
        ws50 = pd.to_numeric(article_variants.get("water_seal_mm", pd.Series(dtype=object)), errors="coerce").eq(50)
        dn50 = _norm_series(article_variants, "outlet_dn").str.upper().str.contains("DN50", regex=False)
        body = _norm_series(article_variants, "variant_type").eq("candidate_body_variant")
        found_easyflow_articles = set(_norm_series(article_variants[ws50 & dn50 & body], "article_number"))
        missing_easyflow_articles = sorted(EASYFLOW_WS50_DN50_EXPECTED_ARTICLES - found_easyflow_articles)
        results.append(CheckResult(
            "article_variants_easyflow_ws50_dn50_articles",
            not missing_easyflow_articles,
            f"missing={missing_easyflow_articles} expected={sorted(EASYFLOW_WS50_DN50_EXPECTED_ARTICLES)}",
        ))

    if final_assemblies is not None:
        final_ids = _norm_series(final_assemblies, "product_id").str.lower()
        assembled_prefix_matches = int(final_ids.str.startswith(FINAL_ASSEMBLIES_PREFIX).sum())
        all_final_ids_assembled = assembled_prefix_matches == len(final_assemblies)
        results.append(CheckResult(
            "final_assemblies_product_id_prefix",
            all_final_ids_assembled,
            f"actual={assembled_prefix_matches} expected={len(final_assemblies)} prefix={FINAL_ASSEMBLIES_PREFIX}",
        ))
        non_assembled_rows = len(final_assemblies) - assembled_prefix_matches
        results.append(CheckResult("final_assemblies_no_non_assembled_rows", non_assembled_rows == 0, f"actual={non_assembled_rows} expected=0"))

        has_family_col = "assembled_family" in final_assemblies.columns
        results.append(CheckResult("final_assemblies_assembled_family_column", has_family_col, f"present={has_family_col}"))
        final_families = _norm_series(final_assemblies, "assembled_family").str.lower()
        if has_family_col:
            for family, expected in EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS.items():
                actual = int((final_families == family).sum())
                results.append(CheckResult(f"final_assemblies_family_count:{family}", actual == expected, f"actual={actual} expected={expected}"))
        else:
            for family, expected in EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS.items():
                results.append(CheckResult(f"final_assemblies_family_count:{family}", False, f"actual=missing_column expected={expected}"))

        easyflow_mask = final_families.eq("easyflow")
        easyflow = final_assemblies[easyflow_mask]
        for field, expected in FINAL_ASSEMBLIES_EASYFLOW_EXPECTED.items():
            if isinstance(expected, str):
                matching = _string_series_eq(easyflow, field, expected)
            else:
                matching = _numeric_series_eq(easyflow, field, expected)
            bad = int((~matching).sum())
            results.append(CheckResult(f"final_assemblies_easyflow_value:{field}", bad == 0, f"actual_bad={bad} expected_bad=0 expected={expected}"))

        missing_columns = [
            col for col in FINAL_ASSEMBLIES_REQUIRED_COMPLETENESS_COLUMNS
            if col not in final_assemblies.columns
        ]
        results.append(CheckResult(
            "final_assemblies_completeness_columns",
            not missing_columns,
            f"missing={missing_columns} expected={FINAL_ASSEMBLIES_REQUIRED_COMPLETENESS_COLUMNS}",
        ))

        statuses = _norm_series(final_assemblies, "data_quality_status").str.lower()
        for status, expected in EXPECTED_FINAL_ASSEMBLIES_STATUS_COUNTS.items():
            actual = int((statuses == status).sum())
            results.append(CheckResult(
                f"final_assemblies_status_count:{status}",
                actual == expected,
                f"actual={actual} expected={expected}",
            ))

        easyflow_status_bad = int((~_string_series_eq(easyflow, "data_quality_status", "partial")).sum())
        results.append(CheckResult(
            "final_assemblies_easyflow_status_partial",
            easyflow_status_bad == 0,
            f"actual_bad={easyflow_status_bad} expected_bad=0",
        ))
        easyflow_complete_bad = int((~_bool_series_eq(easyflow, "is_complete_technical_data", False)).sum())
        results.append(CheckResult(
            "final_assemblies_easyflow_is_complete_false",
            easyflow_complete_bad == 0,
            f"actual_bad={easyflow_complete_bad} expected_bad=0",
        ))
        easyflow_missing_bad = int((~_string_series_eq(
            easyflow,
            "missing_technical_fields",
            FINAL_ASSEMBLIES_EASYFLOW_MISSING_FIELDS,
        )).sum())
        results.append(CheckResult(
            "final_assemblies_easyflow_missing_fields",
            easyflow_missing_bad == 0,
            f"actual_bad={easyflow_missing_bad} expected={FINAL_ASSEMBLIES_EASYFLOW_MISSING_FIELDS}",
        ))
        easyflow_note_mentions = _norm_series(easyflow, "source_status_note").str.lower().str.contains(
            FINAL_ASSEMBLIES_EASYFLOW_NOTE_SNIPPET,
            regex=False,
        )
        easyflow_note_bad = int((~easyflow_note_mentions).sum())
        results.append(CheckResult(
            "final_assemblies_easyflow_source_note",
            easyflow_note_bad == 0,
            f"actual_bad={easyflow_note_bad} expected_snippet={FINAL_ASSEMBLIES_EASYFLOW_NOTE_SNIPPET}",
        ))

        complete_non_easyflow = final_assemblies[~easyflow_mask & statuses.eq("complete")]
        complete_status_bad = int((~_string_series_eq(complete_non_easyflow, "data_quality_status", "complete")).sum())
        complete_flag_bad = int((~_bool_series_eq(complete_non_easyflow, "is_complete_technical_data", True)).sum())
        complete_missing_filled = int((~_empty_series(complete_non_easyflow, "missing_technical_fields")).sum())
        results.append(CheckResult(
            "final_assemblies_complete_non_easyflow_status",
            complete_status_bad == 0,
            f"actual_bad={complete_status_bad} expected_bad=0",
        ))
        results.append(CheckResult(
            "final_assemblies_complete_non_easyflow_is_complete_true",
            complete_flag_bad == 0,
            f"actual_bad={complete_flag_bad} expected_bad=0",
        ))
        results.append(CheckResult(
            "final_assemblies_complete_non_easyflow_missing_fields_empty",
            complete_missing_filled == 0,
            f"actual_filled={complete_missing_filled} expected_filled=0",
        ))

        for field in FINAL_ASSEMBLIES_EASYFLOW_EMPTY_FIELDS:
            filled = int((~_empty_series(easyflow, field)).sum())
            results.append(CheckResult(f"final_assemblies_easyflow_empty:{field}", filled == 0, f"actual_filled={filled} expected_filled=0"))

        mplus = final_assemblies[final_families.eq("showerdrain_mplus")]
        mplus_flow_filled = int((~_empty_series(mplus, "flow_rate_lps")).sum())
        results.append(CheckResult("final_assemblies_mplus_flow_rate_lps_empty", mplus_flow_filled == 0, f"actual_filled={mplus_flow_filled} expected_filled=0"))
        for field in ["channel_body_id", "drain_body_id", "grate_id", "source_url_channel_body", "source_url_drain_body", "source_url_grate"]:
            blank = int(_norm_series(mplus, field).eq("").sum())
            results.append(CheckResult(f"final_assemblies_mplus_link:{field}", blank == 0, f"actual_blank={blank} expected_blank=0"))
        mplus_family_bad = int((~_string_series_eq(mplus, "product_family", "showerdrain_mplus")).sum())
        mplus_family_alias_bad = int((~_string_series_eq(mplus, "family", "showerdrain_mplus")).sum())
        mplus_assembled_marker_bad = int((~_bool_series_eq(mplus, "assembled_from_bom", True)).sum())
        mplus_flow_status_bad = int((~_string_series_eq(mplus, "flow_rate_status", "conditional")).sum())
        mplus_status_bad = int((~_string_series_eq(mplus, "data_quality_status", "conditional_parameter_available_production_blocked")).sum())
        mplus_benchmark_bad = int((~_bool_series_eq(mplus, "ready_for_benchmark", False)).sum())
        mplus_customer_bad = int((~_bool_series_eq(mplus, "ready_for_customer_view", False)).sum())
        mplus_blocked_bad = int((~_norm_series(mplus, "blocked_reason").str.lower().str.contains(MPLUS_BLOCKING_REASON_SNIPPET, regex=False)).sum())
        mplus_note_bad = int((~_norm_series(mplus, "source_status_note").str.contains(MPLUS_PRODUCTION_STATUS_NOTE_SNIPPET, regex=False)).sum())
        results.append(CheckResult("final_assemblies_mplus_product_family", mplus_family_bad == 0, f"actual_bad={mplus_family_bad} expected=showerdrain_mplus"))
        results.append(CheckResult("final_assemblies_mplus_family", mplus_family_alias_bad == 0, f"actual_bad={mplus_family_alias_bad} expected=showerdrain_mplus"))
        results.append(CheckResult("final_assemblies_mplus_assembled_from_bom", mplus_assembled_marker_bad == 0, f"actual_bad={mplus_assembled_marker_bad} expected=true"))
        results.append(CheckResult("final_assemblies_mplus_flow_rate_status", mplus_flow_status_bad == 0, f"actual_bad={mplus_flow_status_bad} expected=conditional"))
        results.append(CheckResult("final_assemblies_mplus_data_quality_status", mplus_status_bad == 0, f"actual_bad={mplus_status_bad} expected=conditional_parameter_available_production_blocked"))
        results.append(CheckResult("final_assemblies_mplus_ready_for_benchmark_false", mplus_benchmark_bad == 0, f"actual_bad={mplus_benchmark_bad} expected=false"))
        results.append(CheckResult("final_assemblies_mplus_ready_for_customer_view_false", mplus_customer_bad == 0, f"actual_bad={mplus_customer_bad} expected=false"))
        results.append(CheckResult("final_assemblies_mplus_blocked_reason", mplus_blocked_bad == 0, f"actual_bad={mplus_blocked_bad} expected_snippet={MPLUS_BLOCKING_REASON_SNIPPET}"))
        results.append(CheckResult("final_assemblies_mplus_source_status_note", mplus_note_bad == 0, f"actual_bad={mplus_note_bad} expected_snippet={MPLUS_PRODUCTION_STATUS_NOTE_SNIPPET}"))

    if final_assemblies is not None and EXPECTED_ASSEMBLED_PREFIX_COUNTS.get("aco-assembled-showerdrain-cplus", 0) > 0:
        cplus = final_assemblies[_norm_series(final_assemblies, "assembled_family").str.lower().eq("showerdrain_cplus")]
        cplus_ids = _norm_series(cplus, "product_id")
        results.append(CheckResult("cplus_final_assemblies_count", len(cplus) == 30, f"actual={len(cplus)} expected=30"))
        results.append(CheckResult("cplus_final_assemblies_unique_ids", cplus_ids.nunique() == len(cplus), f"unique={cplus_ids.nunique()} rows={len(cplus)}"))
        results.append(CheckResult("cplus_final_assemblies_prefix", cplus_ids.str.startswith("aco-assembled-showerdrain-cplus-").all(), "all IDs must use the stable C+ prefix"))
        for field in ("base_id", "base_article_number", "base_source_url", "grate_id", "grate_article_number", "grate_source_url", "source_text_or_reason"):
            blank = int(_norm_series(cplus, field).eq("").sum())
            results.append(CheckResult(f"cplus_final_assemblies_link:{field}", blank == 0, f"actual_blank={blank} expected=0"))
        checks = {
            "product_family": "showerdrain_cplus",
            "family": "showerdrain_cplus",
            "assembly_model": "base_x_grate",
            "compatibility_evidence_type": "explicit_catalog_matrix",
            "compatibility_confidence": "high",
            "data_quality_status": "explicit_source_ready_production_assembly",
        }
        for field, expected in checks.items():
            bad = int((~_string_series_eq(cplus, field, expected)).sum())
            results.append(CheckResult(f"cplus_final_assemblies_value:{field}", bad == 0, f"actual_bad={bad} expected={expected}"))
        for field, expected in (("assembled_from_bom", True), ("article_level_compatibility_found", True), ("ready_for_benchmark", True), ("ready_for_customer_view", False), ("customer_view_enabled", False)):
            bad = int((~_bool_series_eq(cplus, field, expected)).sum())
            results.append(CheckResult(f"cplus_final_assemblies_bool:{field}", bad == 0, f"actual_bad={bad} expected={expected}"))
        base_ids = _norm_series(cplus, "base_id")
        grate_ids = _norm_series(cplus, "grate_id")
        grate_articles = _norm_series(cplus, "grate_article_number")
        results.append(CheckResult("cplus_final_assemblies_no_self_reference", ~(base_ids.eq(grate_ids) & base_ids.ne("")).any(), "base_id must differ from grate_id"))
        results.append(CheckResult("cplus_final_assemblies_no_body_as_grate", ~grate_articles.str.startswith("9010.85.").any(), "9010.85.xx body articles are forbidden as grates"))
        results.append(CheckResult("cplus_final_assemblies_no_tile", ~(_norm_series(cplus, "product_name").str.contains("tile", case=False, regex=False) | grate_articles.str.contains("tile", case=False, regex=False)).any(), "Tile articles are out of scope"))
        for base_id, expected in CPLUS_EXPECTED.items():
            rows = cplus[base_ids.eq(base_id)]
            for field, value in expected.items():
                ok = _string_series_eq(rows, field, value).all() if isinstance(value, str) else _numeric_series_eq(rows, field, value).all()
                results.append(CheckResult(f"cplus_final_assemblies_hydraulic:{base_id}:{field}", bool(ok) and len(rows) == 15, f"rows={len(rows)} expected_rows=15 expected_value={value}"))

    if final_set_details is not None:
        missing_columns = [
            col for col in FINAL_SET_DETAILS_REQUIRED_COLUMNS
            if col not in final_set_details.columns
        ]
        results.append(CheckResult(
            "final_set_details_required_columns",
            not missing_columns,
            f"missing={missing_columns} expected={FINAL_SET_DETAILS_REQUIRED_COLUMNS}",
        ))

        actual = len(final_set_details)
        results.append(CheckResult(
            "final_set_details_row_count",
            actual == EXPECTED_FINAL_SET_DETAILS_ROW_COUNT,
            f"actual={actual} expected={EXPECTED_FINAL_SET_DETAILS_ROW_COUNT}",
        ))

        detail_ids = _norm_series(final_set_details, "assembled_product_id").str.lower()
        prefix_matches = int(detail_ids.str.startswith(FINAL_ASSEMBLIES_PREFIX).sum())
        results.append(CheckResult(
            "final_set_details_assembled_product_id_prefix",
            prefix_matches == len(final_set_details),
            f"actual={prefix_matches} expected={len(final_set_details)} prefix={FINAL_ASSEMBLIES_PREFIX}",
        ))
        set_ids_empty = int(_norm_series(final_set_details, "set_id").eq("").sum())
        detail_ids_empty = int(_norm_series(final_set_details, "assembled_product_id").eq("").sum())
        results.append(CheckResult("final_set_details_set_id_non_empty", set_ids_empty == 0, f"actual_empty={set_ids_empty} expected_empty=0"))
        results.append(CheckResult("final_set_details_assembled_product_id_non_empty", detail_ids_empty == 0, f"actual_empty={detail_ids_empty} expected_empty=0"))

        detail_families = _norm_series(final_set_details, "assembled_family").str.lower()
        for family, expected in EXPECTED_FINAL_SET_DETAILS_FAMILY_COUNTS.items():
            actual = int((detail_families == family).sum())
            results.append(CheckResult(f"final_set_details_family_count:{family}", actual == expected, f"actual={actual} expected={expected}"))

        for field in ["ready_for_benchmark", "ready_for_customer_view"]:
            expected_counts = dict(EXPECTED_FINAL_SET_DETAILS_READY_COUNTS)
            if field == "ready_for_customer_view":
                cplus_count = EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS.get("showerdrain_cplus", 0)
                expected_counts[True] = expected_counts.get(True, 0) - cplus_count
                expected_counts[False] = expected_counts.get(False, 0) + cplus_count
            for expected_bool, expected_count in expected_counts.items():
                actual = int(_bool_series_eq(final_set_details, field, expected_bool).sum())
                results.append(CheckResult(
                    f"final_set_details_{field}_count:{expected_bool}",
                    actual == expected_count,
                    f"actual={actual} expected={expected_count}",
                ))

        easyflow_mask = detail_families.eq("easyflow")
        mplus_detail_mask = detail_families.eq("showerdrain_mplus")
        easyflow = final_set_details[easyflow_mask]
        mplus_details = final_set_details[mplus_detail_mask]
        cplus_detail_mask = detail_families.eq("showerdrain_cplus")
        cplus_details = final_set_details[cplus_detail_mask]
        non_easyflow = final_set_details[~easyflow_mask & ~mplus_detail_mask & ~cplus_detail_mask]
        easyflow_status_bad = int((~_string_series_eq(easyflow, "data_quality_status", "partial")).sum())
        easyflow_benchmark_bad = int((~_bool_series_eq(easyflow, "ready_for_benchmark", False)).sum())
        easyflow_customer_bad = int((~_bool_series_eq(easyflow, "ready_for_customer_view", False)).sum())
        easyflow_blocked_bad = int((~_norm_series(easyflow, "blocked_reason").str.lower().str.contains(FINAL_SET_DETAILS_EASYFLOW_BLOCKED_SNIPPET, regex=False)).sum())
        easyflow_variant_bad = int((~_string_series_eq(easyflow, "article_variant_status", "multiple_candidate_articles")).sum())
        results.append(CheckResult("final_set_details_easyflow_status_partial", easyflow_status_bad == 0, f"actual_bad={easyflow_status_bad} expected_bad=0"))
        results.append(CheckResult("final_set_details_easyflow_ready_for_benchmark_false", easyflow_benchmark_bad == 0, f"actual_bad={easyflow_benchmark_bad} expected_bad=0"))
        results.append(CheckResult("final_set_details_easyflow_ready_for_customer_view_false", easyflow_customer_bad == 0, f"actual_bad={easyflow_customer_bad} expected_bad=0"))
        results.append(CheckResult("final_set_details_easyflow_blocked_reason", easyflow_blocked_bad == 0, f"actual_bad={easyflow_blocked_bad} expected_snippet={FINAL_SET_DETAILS_EASYFLOW_BLOCKED_SNIPPET}"))
        results.append(CheckResult("final_set_details_easyflow_article_variant_status", easyflow_variant_bad == 0, f"actual_bad={easyflow_variant_bad} expected=multiple_candidate_articles"))

        non_easyflow_status_bad = int((~_string_series_eq(non_easyflow, "data_quality_status", "complete")).sum())
        non_easyflow_benchmark_bad = int((~_bool_series_eq(non_easyflow, "ready_for_benchmark", True)).sum())
        non_easyflow_customer_bad = int((~_bool_series_eq(non_easyflow, "ready_for_customer_view", True)).sum())
        non_easyflow_blocked_filled = int((~_empty_series(non_easyflow, "blocked_reason")).sum())
        non_easyflow_variant_bad = int((~_string_series_eq(non_easyflow, "article_variant_status", "not_required")).sum())
        results.append(CheckResult("final_set_details_non_easyflow_status_complete", non_easyflow_status_bad == 0, f"actual_bad={non_easyflow_status_bad} expected_bad=0"))
        results.append(CheckResult("final_set_details_non_easyflow_ready_for_benchmark_true", non_easyflow_benchmark_bad == 0, f"actual_bad={non_easyflow_benchmark_bad} expected_bad=0"))
        results.append(CheckResult("final_set_details_non_easyflow_ready_for_customer_view_true", non_easyflow_customer_bad == 0, f"actual_bad={non_easyflow_customer_bad} expected_bad=0"))
        results.append(CheckResult("final_set_details_non_easyflow_blocked_reason_empty", non_easyflow_blocked_filled == 0, f"actual_filled={non_easyflow_blocked_filled} expected_filled=0"))
        results.append(CheckResult("final_set_details_non_easyflow_article_variant_status", non_easyflow_variant_bad == 0, f"actual_bad={non_easyflow_variant_bad} expected=not_required"))

        cplus_detail_status_bad = int((~_string_series_eq(cplus_details, "data_quality_status", "explicit_source_ready_production_assembly")).sum())
        cplus_detail_benchmark_bad = int((~_bool_series_eq(cplus_details, "ready_for_benchmark", True)).sum())
        cplus_detail_customer_bad = int((~_bool_series_eq(cplus_details, "ready_for_customer_view", False)).sum())
        cplus_detail_blocked_filled = int((~_empty_series(cplus_details, "blocked_reason")).sum())
        results.append(CheckResult("final_set_details_cplus_status", cplus_detail_status_bad == 0, f"actual_bad={cplus_detail_status_bad} expected=explicit_source_ready_production_assembly"))
        results.append(CheckResult("final_set_details_cplus_ready_for_benchmark_true", cplus_detail_benchmark_bad == 0, f"actual_bad={cplus_detail_benchmark_bad} expected=false"))
        results.append(CheckResult("final_set_details_cplus_ready_for_customer_view_false", cplus_detail_customer_bad == 0, f"actual_bad={cplus_detail_customer_bad} expected=false"))
        results.append(CheckResult("final_set_details_cplus_blocked_reason_empty", cplus_detail_blocked_filled == 0, f"actual_filled={cplus_detail_blocked_filled} expected=0"))

        mplus_detail_status_bad = int((~_string_series_eq(mplus_details, "data_quality_status", "conditional_parameter_available_production_blocked")).sum())
        mplus_detail_benchmark_bad = int((~_bool_series_eq(mplus_details, "ready_for_benchmark", False)).sum())
        mplus_detail_customer_bad = int((~_bool_series_eq(mplus_details, "ready_for_customer_view", False)).sum())
        mplus_detail_blocked_bad = int((~_norm_series(mplus_details, "blocked_reason").str.lower().str.contains(MPLUS_BLOCKING_REASON_SNIPPET, regex=False)).sum())
        results.append(CheckResult("final_set_details_mplus_status_conditional_blocked", mplus_detail_status_bad == 0, f"actual_bad={mplus_detail_status_bad} expected=conditional_parameter_available_production_blocked"))
        results.append(CheckResult("final_set_details_mplus_ready_for_benchmark_false", mplus_detail_benchmark_bad == 0, f"actual_bad={mplus_detail_benchmark_bad} expected=false"))
        results.append(CheckResult("final_set_details_mplus_ready_for_customer_view_false", mplus_detail_customer_bad == 0, f"actual_bad={mplus_detail_customer_bad} expected=false"))
        results.append(CheckResult("final_set_details_mplus_blocked_reason", mplus_detail_blocked_bad == 0, f"actual_bad={mplus_detail_blocked_bad} expected_snippet={MPLUS_BLOCKING_REASON_SNIPPET}"))

    cmp_product_ids = _norm_series(comparison, "product_id").str.lower()
    for prefix, expected in EXPECTED_ASSEMBLED_PREFIX_COUNTS.items():
        actual = int(cmp_product_ids.str.startswith(prefix).sum())
        results.append(CheckResult(f"assembled_count:{prefix}", actual == expected, f"actual={actual} expected={expected}"))

    for sheet_name, df in [("Products", products), ("Comparison", comparison)]:
        roles = _norm_series(df, "system_role").str.lower()
        bad = int(roles.isin(FORBIDDEN_SYSTEM_ROLES).sum())
        results.append(CheckResult(f"forbidden_roles:{sheet_name}", bad == 0, f"actual={bad} expected=0"))

    option_types = _norm_series(bom, "option_type").str.lower()
    for option_type, expected in EXPECTED_BOM_OPTION_TYPE_COUNTS.items():
        actual = int((option_types == option_type).sum())
        results.append(CheckResult(f"bom_option_type_count:{option_type}", actual == expected, f"actual={actual} expected={expected}"))

    bom_pid = _norm_series(bom, "product_id")
    bom_cid = _norm_series(bom, "component_id")
    self_refs = int((bom_pid == bom_cid).sum())
    results.append(CheckResult("bom_self_reference", self_refs == 0, f"actual={self_refs} expected=0"))

    comp_roles = _norm_series(components, "system_role").str.lower()
    comp_role_map = {_norm_series(components, "product_id").iloc[i]: comp_roles.iloc[i] for i in range(len(components))}
    grate_ids = {k for k, v in comp_role_map.items() if v == "grate"}
    grate_to_grate = int(((option_types == "compatible_grate") & bom_cid.isin(grate_ids) & bom_pid.isin(grate_ids)).sum())
    results.append(CheckResult("grate_to_grate_links", grate_to_grate == 0, f"actual={grate_to_grate} expected=0"))

    expected_cplus_assemblies = EXPECTED_ASSEMBLED_PREFIX_COUNTS.get("aco-assembled-showerdrain-cplus", 0)
    cplus_bom = bom[(option_types == "compatible_grate") & _norm_series(bom, "product_family").eq("showerdrain_cplus")]
    results.append(CheckResult("cplus_bom_count", len(cplus_bom) == expected_cplus_assemblies, f"actual={len(cplus_bom)} expected={expected_cplus_assemblies}"))
    cplus_bom_components = _norm_series(cplus_bom, "component_id")
    results.append(CheckResult("cplus_bom_component_id_nonempty", cplus_bom_components.ne("").all(), f"empty={int(cplus_bom_components.eq('').sum())}"))
    results.append(CheckResult("cplus_bom_component_id_exists", cplus_bom_components.isin(set(_norm_series(components, "product_id"))).all(), "all C+ component IDs must exist"))
    results.append(CheckResult("cplus_bom_explicit_evidence", _string_series_eq(cplus_bom, "compatibility_evidence_type", "explicit_catalog_matrix").all(), "expected explicit_catalog_matrix"))
    results.append(CheckResult("cplus_bom_high_confidence", _string_series_eq(cplus_bom, "compatibility_confidence", "high").all(), "expected high"))
    results.append(CheckResult("cplus_bom_article_level", _bool_series_eq(cplus_bom, "article_level_compatibility_found", True).all(), "expected true"))

    nav_labels = int(comp_roles.eq("navigation-label").sum())
    results.append(CheckResult("navigation_label_components", nav_labels == 0, f"actual={nav_labels} expected=0"))

    universe = set(_norm_series(products, "product_id")).union(set(_norm_series(components, "product_id")))
    aco_mask = _norm_series(bom, "manufacturer").str.lower().eq("aco")
    dangling = int((~bom_cid.isin(universe) & aco_mask).sum())
    results.append(CheckResult("aco_dangling_component_id", dangling == 0, f"actual={dangling} expected=0"))

    compat = bom[option_types == "compatible_grate"].copy()
    bad_meta = 0
    for _, row in compat.iterrows():
        if not _compatible_grate_meta_valid(row.get("option_meta", "")):
            bad_meta += 1
    results.append(CheckResult("compatible_grate_metadata", bad_meta == 0, f"actual={bad_meta} expected=0"))

    for df_name, df in [("Products", products), ("Comparison", comparison)]:
        ids = set(_norm_series(df, "product_id"))
        missing_ids = [pid for pid in CPLUS_EXPECTED if pid not in ids]
        results.append(CheckResult(f"cplus_presence:{df_name}", not missing_ids, f"missing={missing_ids}"))

    product_indexed = products.set_index("product_id") if "product_id" in products.columns else pd.DataFrame()
    for pid, expected_fields in CPLUS_EXPECTED.items():
        if pid not in product_indexed.index:
            continue
        row = product_indexed.loc[pid]
        for field, expected in expected_fields.items():
            actual = row.get(field)
            passed = str(actual) == str(expected) if isinstance(expected, str) else float(actual) == float(expected)
            results.append(CheckResult(f"cplus_value:{pid}:{field}", passed, f"actual={actual} expected={expected}"))

    all_passed = all(r.passed for r in results)
    xls.close()
    return all_passed, results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate ACO benchmark XLSX export baseline.")
    parser.add_argument("xlsx_path", help="Path to benchmark export XLSX")
    args = parser.parse_args(argv)

    passed, results = validate_xlsx(args.xlsx_path)

    print("XLSX validation report")
    print("=" * 80)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.name}: {r.detail}")
    print("=" * 80)
    print("OVERALL: PASS" if passed else "OVERALL: FAIL")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
