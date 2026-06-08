# src/excel_export.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional
import json
import re

import pandas as pd
import openpyxl

from src.scenario_scoring import build_scenario_comparison, scoring_scenarios_dataframe

_ILLEGAL_EXCEL_XML_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
_ILLEGAL_ESCAPED_UNICODE_RE = re.compile(r"\\u00(?:0[0-8BCEFbcef]|1[0-9A-Fa-f])")
_EXCEL_MAX_CELL_LEN = 32767

BENCHMARK_SCORING_KEYS = [
    "flow_rate_score",
    "material_v4a_score",
    "din_en_1253_score",
    "din_en_18534_score",
    "height_adjustability_score",
    "sales_price_score",
    "outlet_flexibility_score",
    "sealing_fleece_score",
    "colour_count_score",
]

DEFAULT_BENCHMARK_WEIGHTS_PCT = {
    "flow_rate_score": 25,
    "material_v4a_score": 15,
    "din_en_1253_score": 10,
    "din_en_18534_score": 10,
    "height_adjustability_score": 10,
    "sales_price_score": 15,
    "outlet_flexibility_score": 5,
    "sealing_fleece_score": 5,
    "colour_count_score": 5,
}

LEGACY_EQUIVALENCE_KEYS = [
    "length_mode_match",
    "selected_length_match",
    "length_delta_within_tolerance",
    "equiv_finish_set_requires_base",
    "equiv_complete_system_bonus",
]


ASSEMBLED_PREFIX = "aco-assembled-"
FINAL_ASSEMBLIES_REQUIRED_TECHNICAL_FIELDS = [
    "flow_rate_lps",
    "water_seal_mm",
    "outlet_dn",
    "height_adj_min_mm",
    "height_adj_max_mm",
]
EASYFLOW_AMBIGUOUS_TECHNICAL_FIELDS = {
    "flow_rate_lps",
    "height_adj_min_mm",
    "height_adj_max_mm",
}
EASYFLOW_AMBIGUOUS_STATUS_NOTE = (
    "WS/DN inherited from base row; flow/height ambiguous at current "
    "article/variant granularity"
)

ARTICLE_VARIANT_COLUMNS = [
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

FINAL_SET_DETAILS_COLUMNS = [
    "set_id",
    "assembled_product_id",
    "assembled_family",
    "manufacturer",
    "product_name",
    "base_product_id",
    "component_id",
    "component_role",
    "component_family",
    "base_article_number",
    "base_source_url",
    "grate_id",
    "grate_article_number",
    "grate_source_url",
    "source_text_or_reason",
    "compatibility_evidence_type",
    "compatibility_confidence",
    "assembly_model",
    "assembled_from_bom",
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

FINAL_SET_DETAILS_EASYFLOW_BLOCKED_REASON = (
    "flow/height ambiguous at current article/variant granularity"
)
FINAL_SET_DETAILS_EASYFLOW_ARTICLE_NOTE = (
    "matching Article_Variants include multiple WS50/DN50 candidates; "
    "no article variant is selected by default"
)
FINAL_SET_DETAILS_NON_EASYFLOW_ARTICLE_NOTE = "not required for current assembled set"



EPLUS_PROPOSAL_MAPPING_COLUMNS = [
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

EPLUS_BODY_PROPOSALS = (
    (
        "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/"
        "aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/",
        25,
    ),
    (
        "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/"
        "aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/",
        57,
    ),
    (
        "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1",
        "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/"
        "aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1/",
        80,
    ),
)
EPLUS_GRATE_ID = "aco-showerdrain-eplus-design-roste-aus-elektropoliertem-edelstahl"
EPLUS_GRATE_SOURCE_URL = (
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/"
    "aco-showerdrain-eplus/design-roste-aus-elektropoliertem-edelstahl/"
)
EPLUS_COMPATIBILITY_EVIDENCE_TYPE = "page_level_family_bom_or_inferred_from_current_bom"
EPLUS_MISSING_EVIDENCE = "explicit_article_level_base_to_grate_compatibility"
EPLUS_BLOCKING_REASON = (
    "E+ diagnostic has only conservative page-level body/grate evidence and no explicit "
    "article-level base-to-grate compatibility matrix."
)
EPLUS_RECOMMENDED_NEXT_ACTION = (
    "collect explicit article-level E+ base-to-grate compatibility before production generation."
)
EPLUS_PRODUCTION_STATUS_NOTE = (
    "diagnostic/proposal-only; no Products/BOM/assembly generation change"
)


MPLUS_COMPOUND_MAPPING_COLUMNS = [
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

MPLUS_DIAGNOSTIC_ARTICLE_DEFAULTS = {
    "9010.81.20": {"water_seal_mm": "50", "outlet_dn": "DN40/DN50"},
    "9010.81.21": {"water_seal_mm": "30", "outlet_dn": "DN40/DN50"},
    "9010.81.22": {"water_seal_mm": "25", "outlet_dn": "DN40"},
    "9010.81.23": {"water_seal_mm": "50", "outlet_dn": "DN50"},
}
MPLUS_CHANNEL_BODY_ID = "channel-body-25-128"
MPLUS_GRATE_ID = "mplus-design-roste-elektropoliert"
MPLUS_CHANNEL_BODY_SOURCE_URL = (
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/"
    "aco-showerdrain-mplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/"
)
MPLUS_DRAIN_BODY_SOURCE_URL = (
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/"
    "aco-showerdrain-mplus/ablaufkoerper-zur-duschrinne-aco-showerdrain-mplus/"
)
MPLUS_GRATE_SOURCE_URL = (
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/"
    "aco-showerdrain-mplus/design-roste-aus-elektropoliertem-edelstahl/"
)
MPLUS_RECOMMENDED_NEXT_ACTION = (
    "implement scoring/export handling for conditional parameter values before production M+ assemblies"
)
MPLUS_PRODUCTION_STATUS_NOTE = (
    "conditional flow values available in Conditional_Technical_Values; scenario scoring not implemented"
)
MPLUS_FINAL_DATA_QUALITY_STATUS = "conditional_parameter_available_production_blocked"
MPLUS_FLOW_RATE_STATUS = "conditional"
MPLUS_BLOCKING_REASON = "blocked_pending_conditional_parameter_scoring"

CONDITIONAL_TECHNICAL_VALUES_COLUMNS = [
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

MPLUS_CONDITIONAL_FLOW_VARIANTS = (
    {
        "parameter_name": "flow_rate_lps",
        "value": 0.4,
        "unit": "l/s",
        "condition_type": "head_water_level",
        "condition_value": 10,
        "condition_unit": "mm",
        "condition_label": "10 mm head water level",
        "mplus_source_column": "flow_rate_lps_10mm_head",
    },
    {
        "parameter_name": "flow_rate_lps",
        "value": 0.46,
        "unit": "l/s",
        "condition_type": "head_water_level",
        "condition_value": 20,
        "condition_unit": "mm",
        "condition_label": "20 mm head water level",
        "mplus_source_column": "flow_rate_lps_20mm_head",
    },
)

def _assembled_family(product_id: Any) -> str:
    pid = str(product_id or "")
    if pid.startswith("aco-assembled-showerdrain-mplus-"):
        return "showerdrain_mplus"
    if pid.startswith("aco-assembled-showerdrain-cplus-"):
        return "showerdrain_cplus"
    if pid.startswith("aco-assembled-easyflowplus-"):
        return "easyflowplus"
    if pid.startswith("aco-assembled-easyflow-"):
        return "easyflow"
    if pid.startswith("aco-assembled-showerdrain-c-"):
        return "showerdrain_c"
    if pid.startswith("aco-assembled-showerdrain-splus-"):
        return "showerdrain_splus"
    return "unknown"


def _missing_final_assembly_technical_fields(row: pd.Series) -> list[str]:
    return [
        field
        for field in FINAL_ASSEMBLIES_REQUIRED_TECHNICAL_FIELDS
        if not _present(row.get(field))
    ]


def _final_assembly_data_quality_status(missing_fields: list[str], family: str = "") -> str:
    if family == "showerdrain_mplus":
        return MPLUS_FINAL_DATA_QUALITY_STATUS
    if family == "showerdrain_cplus" and not missing_fields:
        return "explicit_source_ready_production_assembly"
    if not missing_fields:
        return "complete"
    if len(missing_fields) == len(FINAL_ASSEMBLIES_REQUIRED_TECHNICAL_FIELDS):
        return "missing"
    return "partial"


def _final_assembly_source_status_note(family: str, missing_fields: list[str]) -> str:
    if family == "showerdrain_mplus":
        return MPLUS_PRODUCTION_STATUS_NOTE
    if family == "showerdrain_cplus" and not missing_fields:
        return (
            "explicit C+ catalog matrix on pages 25-26; protected C+ body and stainless "
            "grate articles are compatible only at equal nominal length"
        )
    if not missing_fields:
        return "complete technical data"
    if family == "easyflow" and EASYFLOW_AMBIGUOUS_TECHNICAL_FIELDS.issubset(set(missing_fields)):
        return EASYFLOW_AMBIGUOUS_STATUS_NOTE
    return "partial technical data"


def _parse_final_set_parts(product_id: Any, family: str) -> tuple[str, str]:
    pid = str(product_id or "").strip()
    if not pid.startswith(ASSEMBLED_PREFIX):
        return "", ""

    family_prefixes = {
        "easyflowplus": "aco-assembled-easyflowplus-",
        "easyflow": "aco-assembled-easyflow-",
        "showerdrain_c": "aco-assembled-showerdrain-c-",
        "showerdrain_splus": "aco-assembled-showerdrain-splus-",
        "showerdrain_mplus": "aco-assembled-showerdrain-mplus-",
        "showerdrain_cplus": "aco-assembled-showerdrain-cplus-",
    }
    prefix = family_prefixes.get(str(family or ""), ASSEMBLED_PREFIX)
    remainder = pid[len(prefix):] if pid.startswith(prefix) else pid[len(ASSEMBLED_PREFIX):]
    if not remainder or "__" not in remainder:
        return "", ""

    base_product_id, component_id = remainder.split("__", 1)
    return base_product_id.strip(), component_id.strip()


def _first_present(row: pd.Series, columns: tuple[str, ...]) -> Any:
    for column in columns:
        value = row.get(column)
        if _present(value):
            return value
    return ""


def _build_component_lookup(
    bom_options_df: pd.DataFrame,
    components_df: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    bom_options_df = pd.DataFrame() if bom_options_df is None else bom_options_df.copy()
    components_df = pd.DataFrame() if components_df is None else components_df.copy()
    if bom_options_df.empty or "product_id" not in bom_options_df.columns:
        return {}

    component_rows: dict[str, pd.Series] = {}
    if not components_df.empty and "product_id" in components_df.columns:
        for _, component in components_df.iterrows():
            component_id = str(component.get("product_id") or "").strip()
            if component_id and component_id not in component_rows:
                component_rows[component_id] = component

    lookup: dict[str, dict[str, Any]] = {}
    for product_id, group in bom_options_df.groupby(bom_options_df["product_id"].fillna("").astype(str)):
        product_id = str(product_id or "").strip()
        if not product_id or len(group) != 1:
            continue
        row = group.iloc[0]
        component_id = str(row.get("component_id") or "").strip()
        if not component_id:
            continue
        component = component_rows.get(component_id)
        component_role = _first_present(row, ("component_role", "option_role", "system_role"))
        component_family = _first_present(row, ("component_family", "family", "product_family"))
        if component is not None:
            if not _present(component_role):
                component_role = _first_present(component, ("component_role", "system_role", "candidate_type"))
            if not _present(component_family):
                component_family = _first_present(component, ("component_family", "family", "product_family"))
        lookup[product_id] = {
            "component_id": component_id,
            "component_role": component_role,
            "component_family": component_family,
        }
    return lookup


def _extract_final_set_details(
    final_assemblies_df: pd.DataFrame,
    bom_options_df: pd.DataFrame,
    components_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return export-only detail rows for already-created final assemblies."""
    final_assemblies_df = pd.DataFrame() if final_assemblies_df is None else final_assemblies_df.copy()
    if final_assemblies_df.empty:
        return pd.DataFrame(columns=FINAL_SET_DETAILS_COLUMNS)

    product_col = "product_id" if "product_id" in final_assemblies_df.columns else "assembled_product_id"
    assembled_mask = final_assemblies_df.get(product_col, pd.Series([""] * len(final_assemblies_df), index=final_assemblies_df.index)).fillna("").astype(str).str.startswith(ASSEMBLED_PREFIX)
    final_assemblies_df = final_assemblies_df[assembled_mask].copy()
    bom_lookup = _build_component_lookup(bom_options_df, components_df)
    component_lookup = {
        str(row.get("product_id") or "").strip(): row
        for _, row in components_df.iterrows()
        if str(row.get("product_id") or "").strip()
    }

    rows: list[dict[str, Any]] = []
    for _, assembly in final_assemblies_df.iterrows():
        assembled_product_id = str(assembly.get(product_col) or "").strip()
        assembled_family = str(assembly.get("assembled_family") or _assembled_family(assembled_product_id)).strip()
        base_product_id, component_id = _parse_final_set_parts(assembled_product_id, assembled_family)
        component_role = ""
        component_family = ""
        lookup = bom_lookup.get(assembled_product_id, {})
        if lookup:
            component_id = lookup.get("component_id") or component_id
            component_role = lookup.get("component_role") or ""
            component_family = lookup.get("component_family") or ""
        component = component_lookup.get(component_id)
        if component is not None:
            if not _present(component_role):
                component_role = _first_present(component, ("component_role", "system_role", "candidate_type"))
            if not _present(component_family):
                component_family = _first_present(component, ("component_family", "family", "product_family"))

        data_quality_status = str(assembly.get("data_quality_status") or "").strip().lower()
        is_complete = data_quality_status in {"complete", "explicit_source_ready_production_assembly"}
        is_easyflow_partial = assembled_family == "easyflow" and data_quality_status == "partial"
        is_mplus_conditional = assembled_family == "showerdrain_mplus"
        def _assembly_bool(value: Any, default: bool) -> bool:
            if value is None or _is_nan(value):
                return default
            text = str(value).strip().lower()
            if text in {"true", "1", "yes", "y", "ja"}:
                return True
            if text in {"false", "0", "no", "n", "nein"}:
                return False
            return default

        ready_for_benchmark = _assembly_bool(assembly.get("ready_for_benchmark", None), is_complete)
        ready_for_customer_view = _assembly_bool(assembly.get("ready_for_customer_view", None), is_complete)
        if is_mplus_conditional:
            ready_for_benchmark = False
            ready_for_customer_view = False

        rows.append({
            "set_id": assembled_product_id,
            "assembled_product_id": assembled_product_id,
            "assembled_family": assembled_family,
            "manufacturer": assembly.get("manufacturer", ""),
            "product_name": assembly.get("product_name", assembly.get("name", "")),
            "base_product_id": base_product_id,
            "component_id": component_id,
            "component_role": component_role,
            "component_family": component_family,
            "base_article_number": assembly.get("base_article_number", ""),
            "base_source_url": assembly.get("base_source_url", ""),
            "grate_id": assembly.get("grate_id", component_id),
            "grate_article_number": assembly.get("grate_article_number", ""),
            "grate_source_url": assembly.get("grate_source_url", ""),
            "source_text_or_reason": assembly.get("source_text_or_reason", ""),
            "compatibility_evidence_type": assembly.get("compatibility_evidence_type", ""),
            "compatibility_confidence": assembly.get("compatibility_confidence", ""),
            "assembly_model": assembly.get("assembly_model", ""),
            "assembled_from_bom": assembly.get("assembled_from_bom", ""),
            "flow_rate_lps": assembly.get("flow_rate_lps", ""),
            "water_seal_mm": assembly.get("water_seal_mm", ""),
            "outlet_dn": assembly.get("outlet_dn", ""),
            "height_adj_min_mm": assembly.get("height_adj_min_mm", ""),
            "height_adj_max_mm": assembly.get("height_adj_max_mm", ""),
            "is_complete_technical_data": assembly.get("is_complete_technical_data", is_complete),
            "missing_technical_fields": assembly.get("missing_technical_fields", ""),
            "data_quality_status": data_quality_status,
            "source_status_note": assembly.get("source_status_note", ""),
            "ready_for_benchmark": ready_for_benchmark,
            "ready_for_customer_view": ready_for_customer_view,
            "blocked_reason": MPLUS_BLOCKING_REASON if is_mplus_conditional else (FINAL_SET_DETAILS_EASYFLOW_BLOCKED_REASON if is_easyflow_partial else ""),
            "article_variant_status": "conditional_parameter_available" if is_mplus_conditional else ("multiple_candidate_articles" if is_easyflow_partial else "not_required"),
            "article_variant_note": MPLUS_PRODUCTION_STATUS_NOTE if is_mplus_conditional else (FINAL_SET_DETAILS_EASYFLOW_ARTICLE_NOTE if is_easyflow_partial else FINAL_SET_DETAILS_NON_EASYFLOW_ARTICLE_NOTE),
            "product_url": assembly.get("product_url", ""),
            "source_url": assembly.get("source_url", ""),
            "sources": assembly.get("sources", ""),
        })

    return pd.DataFrame(rows, columns=FINAL_SET_DETAILS_COLUMNS)


def _extract_final_assemblies(products_df: pd.DataFrame) -> pd.DataFrame:
    """Return final assembled ACO products for the Final_Assemblies export sheet."""
    products_df = pd.DataFrame() if products_df is None else products_df.copy()
    completeness_columns = [
        "is_complete_technical_data",
        "missing_technical_fields",
        "data_quality_status",
        "source_status_note",
    ]
    if "product_id" not in products_df.columns:
        return pd.DataFrame(
            columns=["assembled_family", *products_df.columns.tolist(), *completeness_columns]
        )

    product_ids = products_df["product_id"].fillna("").astype(str)
    final_assemblies = products_df[product_ids.str.startswith(ASSEMBLED_PREFIX)].copy()

    families = final_assemblies["product_id"].map(_assembled_family)
    if "assembled_family" in final_assemblies.columns:
        final_assemblies["assembled_family"] = families
    else:
        insert_at = 0
        if "product_id" in final_assemblies.columns:
            insert_at = final_assemblies.columns.get_loc("product_id") + 1
        final_assemblies.insert(insert_at, "assembled_family", families)

    missing_by_row = final_assemblies.apply(
        _missing_final_assembly_technical_fields,
        axis=1,
    )
    statuses = pd.Series(
        [
            _final_assembly_data_quality_status(missing_fields, family)
            for family, missing_fields in zip(families, missing_by_row)
        ],
        index=final_assemblies.index,
    )

    final_assemblies["is_complete_technical_data"] = statuses.isin({
        "complete", "explicit_source_ready_production_assembly"
    })
    final_assemblies["missing_technical_fields"] = missing_by_row.map(",".join)
    final_assemblies["data_quality_status"] = statuses
    final_assemblies["source_status_note"] = [
        _final_assembly_source_status_note(family, missing_fields)
        for family, missing_fields in zip(families, missing_by_row)
    ]

    mplus_mask = families.eq("showerdrain_mplus")
    if bool(mplus_mask.any()):
        for column, value in {
            "product_family": "showerdrain_mplus",
            "family": "showerdrain_mplus",
            "assembled_from_bom": True,
            "system_role": "assembled_system",
            "flow_rate_lps": "",
            "flow_rate_status": MPLUS_FLOW_RATE_STATUS,
            "is_complete_technical_data": False,
            "missing_technical_fields": "flow_rate_lps",
            "data_quality_status": MPLUS_FINAL_DATA_QUALITY_STATUS,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocked_reason": MPLUS_BLOCKING_REASON,
            "source_status_note": MPLUS_PRODUCTION_STATUS_NOTE,
        }.items():
            if column not in final_assemblies.columns:
                final_assemblies[column] = ""
            elif isinstance(value, bool):
                final_assemblies[column] = final_assemblies[column].astype(object)
            final_assemblies.loc[mplus_mask, column] = value

    return final_assemblies


def _is_nan(x: Any) -> bool:
    try:
        return x != x  # NaN != NaN
    except Exception:
        return False


def _sanitize_excel_string(value: str) -> str:
    s = value or ""
    s = _ILLEGAL_EXCEL_XML_CHARS_RE.sub("", s)
    s = _ILLEGAL_ESCAPED_UNICODE_RE.sub("", s)

    if len(s) > _EXCEL_MAX_CELL_LEN:
        s = s[:_EXCEL_MAX_CELL_LEN]

    return s


def _to_excel_cell(v: Any) -> Any:
    """
    Převede hodnotu z DataFrame na hodnotu, kterou openpyxl umí uložit do buňky.

    - None/NaN -> None
    - list/tuple/set/dict -> JSON string
    - Path -> str
    - numpy scalar -> item()
    """
    if v is None or _is_nan(v):
        return None

    if hasattr(v, "item") and callable(getattr(v, "item")):
        try:
            v = v.item()
        except Exception:
            pass

    if isinstance(v, Path):
        return _sanitize_excel_string(str(v))

    if isinstance(v, (bytes, bytearray)):
        try:
            return _sanitize_excel_string(v.decode("utf-8", errors="ignore"))
        except Exception:
            return _sanitize_excel_string(str(v))

    if isinstance(v, (list, tuple, set, dict)):
        try:
            if isinstance(v, set):
                v = sorted(list(v))
            return _sanitize_excel_string(json.dumps(v, ensure_ascii=False))
        except Exception:
            return _sanitize_excel_string(str(v))

    if isinstance(v, str):
        return _sanitize_excel_string(v)

    return v




def _extract_source_checks(evidence_df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "manufacturer", "source_id", "family", "source_url", "source_type", "status_code",
        "final_url", "content_hash_sha256", "content_length", "baseline_hash_sha256",
        "baseline_content_length", "hash_changed", "length_changed", "expected_terms_found",
        "expected_terms_missing", "new_source_candidate_count", "sample_new_source_candidates",
        "review_required", "review_reason", "checked_at", "extraction_mode", "fetch_error",
    ]
    if evidence_df.empty or "label" not in evidence_df.columns or "snippet" not in evidence_df.columns:
        return pd.DataFrame(columns=required_cols)
    rows = []
    for _, ev in evidence_df.iterrows():
        label = str(ev.get("label") or "")
        if not label.startswith("source_check:"):
            continue
        snippet = ev.get("snippet")
        try:
            payload = json.loads(snippet) if isinstance(snippet, str) else {}
        except Exception:
            payload = {}
        row = {k: payload.get(k, "") for k in required_cols}
        if not row.get("manufacturer"):
            row["manufacturer"] = str(ev.get("manufacturer") or "")
        if not row.get("source_id"):
            row["source_id"] = label.split(":", 1)[-1]
        rows.append(row)
    return pd.DataFrame(rows, columns=required_cols)


def _present(v: Any) -> bool:
    if v is None or _is_nan(v):
        return False
    s = str(v).strip().lower()
    return s not in {"", "nan", "none", "null", "unknown", "not_applicable"}



def _cfg_get(cfg: Any, key: str, default: Any = None) -> Any:
    if cfg is None:
        return default
    if hasattr(cfg, "get") and callable(getattr(cfg, "get")):
        try:
            return cfg.get(key, default)
        except Exception:
            pass
    return getattr(cfg, key, default)


def _extract_final_scoring_weights(cfg: Any) -> pd.DataFrame:
    raw = _cfg_get(cfg, "final_weights_pct", {}) or {}
    if not isinstance(raw, dict):
        raw = {}

    rows = []
    for key in BENCHMARK_SCORING_KEYS:
        val = raw.get(key, DEFAULT_BENCHMARK_WEIGHTS_PCT[key])
        try:
            val = float(val)
        except Exception:
            val = float(DEFAULT_BENCHMARK_WEIGHTS_PCT[key])
        rows.append({
            "key": key,
            "weight_pct": val,
            "enabled": True,
            "scoring_model": "benchmark_scoring_v2",
            "note": "active benchmark scoring criterion",
        })

    return pd.DataFrame(rows, columns=["key", "weight_pct", "enabled", "scoring_model", "note"])


def _extract_legacy_equivalence_weights(cfg: Any) -> pd.DataFrame:
    raw = _cfg_get(cfg, "equivalence_weights_pct", {}) or {}
    if not isinstance(raw, dict):
        raw = {}

    rows = []
    for key in LEGACY_EQUIVALENCE_KEYS:
        try:
            legacy_val = float(raw.get(key, 0) or 0)
        except Exception:
            legacy_val = 0.0
        rows.append({
            "key": key,
            "weight_pct": 0.0,
            "legacy_weight_pct": legacy_val,
            "enabled": False,
            "scoring_model": "legacy_equivalence_diagnostic_only",
            "note": "disabled; not used by benchmark_scoring_v2 final_score",
        })

    return pd.DataFrame(rows, columns=["key", "weight_pct", "legacy_weight_pct", "enabled", "scoring_model", "note"])


def _extract_config_sheet() -> pd.DataFrame:
    rows = [
        {"key": "active_scoring_model", "value": "benchmark_scoring_v2", "note": "active scoring model"},
        {"key": "benchmark_scoring_weights_sheet", "value": "Final_Scoring_Weights", "note": "active benchmark scoring weights"},
        {"key": "legacy_equivalence_weights_sheet", "value": "Legacy_Equivalence_Weights", "note": "disabled diagnostic-only legacy weights"},
    ]
    return pd.DataFrame(rows, columns=["key", "value", "note"])
def _scoring_field_coverage(products_df: pd.DataFrame, comparison_df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "manufacturer",
        "product_id",
        "product_name",
        "candidate_type",
        "complete_system",
        "in_products",
        "in_comparison",
        "has_flow_rate_lps",
        "has_material_data",
        "has_din_en_1253_data",
        "has_din_en_18534_data",
        "has_height_adjustability_data",
        "has_price_data",
        "has_outlet_flexibility_data",
        "has_sealing_fleece_data",
        "has_colour_count_data",
        "present_scoring_fields",
        "missing_scoring_fields",
        "scoring_readiness_pct",
        "scoring_readiness_note",
    ]

    products_df = pd.DataFrame() if products_df is None else products_df.copy()
    comparison_df = pd.DataFrame() if comparison_df is None else comparison_df.copy()

    def _present(value: Any) -> bool:
        if value is None:
            return False
        if _is_nan(value):
            return False
        text = str(value).strip().lower()
        if text in {"", "nan", "none", "null", "unknown", "not_applicable", "n/a", "na"}:
            return False
        return True

    def _text(r: pd.Series, keys: tuple[str, ...]) -> str:
        return " ".join(str(r.get(k) or "") for k in keys).lower()

    def _truthy(value: Any) -> bool:
        return str(value or "").strip().lower() in {"yes", "true", "1", "y", "ja"}

    def _has_positive_number(r: pd.Series, key: str) -> bool:
        try:
            value = pd.to_numeric(r.get(key), errors="coerce")
            return pd.notna(value) and float(value) > 0
        except Exception:
            return False

    def _has_height_adjustability(r: pd.Series) -> bool:
        try:
            hmin = pd.to_numeric(r.get("height_adj_min_mm"), errors="coerce")
            hmax = pd.to_numeric(r.get("height_adj_max_mm"), errors="coerce")
            return pd.notna(hmin) and pd.notna(hmax) and float(hmax) > float(hmin)
        except Exception:
            return False

    frames = []
    if not products_df.empty:
        p = products_df.copy()
        p["in_products"] = True
        p["in_comparison"] = False
        frames.append(p)

    if not comparison_df.empty:
        c = comparison_df.copy()
        c["in_products"] = False
        c["in_comparison"] = True
        frames.append(c)

    if not frames:
        return pd.DataFrame(columns=cols)

    all_rows = pd.concat(frames, ignore_index=True, sort=False)

    if "manufacturer" not in all_rows.columns:
        all_rows["manufacturer"] = ""
    if "product_id" not in all_rows.columns:
        all_rows["product_id"] = ""

    all_rows["manufacturer"] = all_rows["manufacturer"].astype(str)
    all_rows["product_id"] = all_rows["product_id"].astype(str)

    all_rows = all_rows.sort_values(
        by=["in_comparison", "in_products"],
        ascending=[False, False],
    )

    all_rows = all_rows.drop_duplicates(
        subset=["manufacturer", "product_id"],
        keep="first",
    )

    # Membership flags must reflect real membership in Products / Comparison,
    # not only the row that survived drop_duplicates.
    product_keys = set()
    if not products_df.empty and {"manufacturer", "product_id"}.issubset(products_df.columns):
        product_keys = set(
            zip(
                products_df["manufacturer"].astype(str),
                products_df["product_id"].astype(str),
            )
        )

    comparison_keys = set()
    if not comparison_df.empty and {"manufacturer", "product_id"}.issubset(comparison_df.columns):
        comparison_keys = set(
            zip(
                comparison_df["manufacturer"].astype(str),
                comparison_df["product_id"].astype(str),
            )
        )

    out = []

    groups = [
        (
            "has_flow_rate_lps",
            lambda r: _has_positive_number(r, "flow_rate_lps"),
        ),
        (
            "has_material_data",
            lambda r: any(
                _present(r.get(k))
                for k in ("material_v4a", "material_detail", "material_class")
            ),
        ),
        (
            "has_din_en_1253_data",
            lambda r: (
                "din en 1253"
                in _text(
                    r,
                    (
                        "din_en_1253",
                        "certification_din_en_1253",
                        "din_en_1253_cert",
                        "certifications",
                        "certificate_text",
                    ),
                )
                or _truthy(r.get("din_en_1253"))
                or _truthy(r.get("din_en_1253_cert"))
            ),
        ),
        (
            "has_din_en_18534_data",
            lambda r: (
                "din en 18534"
                in _text(
                    r,
                    (
                        "din_en_18534",
                        "certification_din_en_18534",
                        "din_18534_compliance",
                        "waterproofing_standard",
                        "certifications",
                        "certificate_text",
                    ),
                )
                or _truthy(r.get("din_en_18534"))
                or _truthy(r.get("din_18534_compliance"))
            ),
        ),
        (
            "has_height_adjustability_data",
            lambda r: _has_height_adjustability(r),
        ),
        (
            "has_price_data",
            lambda r: any(
                _has_positive_number(r, k)
                for k in ("sales_price", "sales_price_eur", "price_eur", "offer_price")
            ),
        ),
        (
            "has_outlet_flexibility_data",
            lambda r: any(
                _present(r.get(k))
                for k in (
                    "vertical_outlet_available",
                    "side_outlet_available",
                    "outlet_orientation",
                    "outlet_options",
                )
            ),
        ),
        (
            "has_sealing_fleece_data",
            lambda r: any(
                _present(r.get(k))
                for k in (
                    "sealing_fleece_preassembled",
                    "sealing_fleece",
                    "waterproofing_fleece_preassembled",
                )
            ),
        ),
        (
            "has_colour_count_data",
            lambda r: any(
                _present(r.get(k))
                for k in (
                    "colours_count",
                    "color_count",
                    "available_colours",
                    "available_colors",
                    "finish_count",
                )
            ),
        ),
    ]

    for _, r in all_rows.iterrows():
        key = (str(r.get("manufacturer") or ""), str(r.get("product_id") or ""))
        flags = {name: bool(fn(r)) for name, fn in groups}

        present = [name for name, value in flags.items() if value]
        missing = [name for name, value in flags.items() if not value]

        rec = {
            c: r.get(c, "")
            for c in (
                "manufacturer",
                "product_id",
                "product_name",
                "candidate_type",
                "complete_system",
            )
        }

        rec["in_products"] = key in product_keys
        rec["in_comparison"] = key in comparison_keys
        rec.update(flags)
        rec["present_scoring_fields"] = ",".join(present)
        rec["missing_scoring_fields"] = ",".join(missing)
        rec["scoring_readiness_pct"] = round((len(present) / len(groups)) * 100.0, 1)
        rec["scoring_readiness_note"] = (
            "ready" if len(missing) == 0 else f"missing:{len(missing)}"
        )

        out.append(rec)

    return pd.DataFrame(out, columns=cols)


def _extract_article_variants(registry_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    combined = " ".join(
        str(value or "")
        for df in (registry_df, products_df)
        for value in (df.astype(str).to_numpy().ravel().tolist() if df is not None and not df.empty else [])
    ).lower()
    if "easyflow" not in combined:
        return pd.DataFrame(columns=ARTICLE_VARIANT_COLUMNS)

    from tools import report_easyflow_article_variants as article_variants

    variants = article_variants.build_article_variants_dataframe(registry_df, products_df)
    return variants.reindex(columns=ARTICLE_VARIANT_COLUMNS)


def _flow_candidate_by_head(flow_diagnostic: Any, head_mm: str) -> Any:
    for candidate in getattr(flow_diagnostic, "drain_body_flow_candidates", ()):
        if str(getattr(candidate, "head_mm", "")).strip() == str(head_mm):
            return candidate
    return None


def _first_accessory_flow_reduction(flow_diagnostic: Any) -> str:
    for reduction in getattr(flow_diagnostic, "accessory_flow_reduction_lps", ()):
        value = getattr(reduction, "accessory_flow_reduction_lps", "")
        if _present(value):
            return str(value).strip()
    return ""


def _mplus_mapping_by_article(mapping_report: Any) -> dict[str, Any]:
    return {
        str(getattr(row, "drain_body_article_number", "")).strip(): row
        for row in getattr(mapping_report, "proposed_mappings", ())
        if str(getattr(row, "drain_body_article_number", "")).strip()
    }


def _mplus_cell(value: Any, fallback: Any = "") -> Any:
    return value if _present(value) else fallback


def _mplus_assembled_product_id(channel_body_id: Any, drain_body_id: Any, grate_id: Any) -> str:
    return (
        f"aco-assembled-showerdrain-mplus-{channel_body_id}__{drain_body_id}__{grate_id}"
        .lower()
        .replace(" ", "-")
    )


def _fallback_mplus_mapping_value(article_number: str, field: str) -> str:
    drain_id = f"aco-{article_number.replace('.', '')}"
    values = {
        "set_id": _mplus_assembled_product_id(MPLUS_CHANNEL_BODY_ID, drain_id, MPLUS_GRATE_ID),
        "product_family": "showerdrain_mplus",
        "assembly_model": "channel_body_x_drain_body_x_grate",
        "channel_body_id": MPLUS_CHANNEL_BODY_ID,
        "channel_body_article_number": "",
        "drain_body_id": drain_id,
        "drain_body_article_number": article_number,
        "grate_id": MPLUS_GRATE_ID,
        "grate_article_number": "",
        "source_url_channel_body": MPLUS_CHANNEL_BODY_SOURCE_URL,
        "source_url_drain_body": MPLUS_DRAIN_BODY_SOURCE_URL,
        "source_url_grate": MPLUS_GRATE_SOURCE_URL,
        "water_seal_mm": MPLUS_DIAGNOSTIC_ARTICLE_DEFAULTS[article_number]["water_seal_mm"],
        "outlet_dn": MPLUS_DIAGNOSTIC_ARTICLE_DEFAULTS[article_number]["outlet_dn"],
        "height_adj_min_mm": "25",
        "height_adj_max_mm": "128",
    }
    return values.get(field, "")


def _extract_eplus_proposal_mappings(
    products_df: pd.DataFrame,
    final_assemblies_df: pd.DataFrame,
    final_set_details_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return diagnostic-only ACO ShowerDrain E+ base_x_grate proposal rows.

    The rows are export-only diagnostics: they deliberately keep all production readiness
    flags false and are not fed back into Products, BOM_Options, or assembly generation.
    """
    rows: list[dict[str, Any]] = []
    for body_id, body_source_url, height_min in EPLUS_BODY_PROPOSALS:
        set_id = f"diagnostic-eplus-{body_id}__{EPLUS_GRATE_ID}"
        rows.append(
            {
                "set_id": set_id,
                "product_family": "showerdrain_eplus",
                "assembly_model": "base_x_grate",
                "body_id": body_id,
                "body_article_number": "",
                "body_source_url": body_source_url,
                "grate_id": EPLUS_GRATE_ID,
                "grate_article_number": "",
                "grate_source_url": EPLUS_GRATE_SOURCE_URL,
                "flow_rate_lps": 0.70,
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "height_adj_min_mm": height_min,
                "height_adj_max_mm": 128,
                "body_evidence_type": "source_page_level_body_url",
                "body_confidence": "high",
                "grate_evidence_type": "source_page_level_grate_url",
                "grate_confidence": "high",
                "compatibility_evidence_type": EPLUS_COMPATIBILITY_EVIDENCE_TYPE,
                "compatibility_confidence": "medium",
                "article_level_compatibility_found": False,
                "data_quality_status": "proposal_only_partial",
                "missing_evidence": EPLUS_MISSING_EVIDENCE,
                "safe_to_generate": False,
                "ready_for_benchmark": False,
                "ready_for_customer_view": False,
                "blocking_reason": EPLUS_BLOCKING_REASON,
                "recommended_next_action": EPLUS_RECOMMENDED_NEXT_ACTION,
                "production_status_note": EPLUS_PRODUCTION_STATUS_NOTE,
            }
        )

    return pd.DataFrame(rows, columns=EPLUS_PROPOSAL_MAPPING_COLUMNS)


def _extract_mplus_compound_mappings(
    registry_df: pd.DataFrame,
    products_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    components_df: pd.DataFrame,
    bom_options_df: pd.DataFrame,
    final_assemblies_df: pd.DataFrame,
    final_set_details_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return diagnostic-only ACO ShowerDrain M+ compound mapping proposal rows."""
    from tools import diagnose_mplus_flow_rate_sources as flow_sources
    from tools import report_mplus_compound_assembly_mapping as compound_mapping
    from tools import report_mplus_flow_rate_policy as flow_policy

    mapping_report = compound_mapping.build_report(
        registry_df,
        products_df,
        comparison_df,
        components_df,
        bom_options_df,
        final_assemblies_df,
        final_set_details_df,
    )
    flow_diagnostic = flow_sources.build_diagnostic()
    policy_report = flow_policy.build_policy_report(
        flow_diagnostic,
        mapping_report,
        use_confirmed_mapping_fallback=True,
    )

    by_article = _mplus_mapping_by_article(mapping_report)
    flow_10mm = _flow_candidate_by_head(flow_diagnostic, "10")
    flow_20mm = _flow_candidate_by_head(flow_diagnostic, "20")
    flow_evidence = flow_10mm or flow_20mm
    accessory_reduction = _first_accessory_flow_reduction(flow_diagnostic)

    rows: list[dict[str, Any]] = []
    for article_number in MPLUS_DIAGNOSTIC_ARTICLE_DEFAULTS:
        mapping = by_article.get(article_number)
        row = {
            "set_id": _fallback_mplus_mapping_value(article_number, "set_id"),
            "product_family": _fallback_mplus_mapping_value(article_number, "product_family"),
            "assembly_model": _fallback_mplus_mapping_value(article_number, "assembly_model"),
            "channel_body_id": _fallback_mplus_mapping_value(article_number, "channel_body_id"),
            "channel_body_article_number": _mplus_cell(getattr(mapping, "channel_body_article_number", ""), _fallback_mplus_mapping_value(article_number, "channel_body_article_number")),
            "drain_body_id": _fallback_mplus_mapping_value(article_number, "drain_body_id"),
            "drain_body_article_number": _mplus_cell(getattr(mapping, "drain_body_article_number", ""), article_number),
            "grate_id": _fallback_mplus_mapping_value(article_number, "grate_id"),
            "grate_article_number": _mplus_cell(getattr(mapping, "grate_article_number", ""), _fallback_mplus_mapping_value(article_number, "grate_article_number")),
            "source_url_channel_body": _mplus_cell(getattr(mapping, "source_url_channel_body", ""), _fallback_mplus_mapping_value(article_number, "source_url_channel_body")),
            "source_url_drain_body": _mplus_cell(getattr(mapping, "source_url_drain_body", ""), _fallback_mplus_mapping_value(article_number, "source_url_drain_body")),
            "source_url_grate": _mplus_cell(getattr(mapping, "source_url_grate", ""), _fallback_mplus_mapping_value(article_number, "source_url_grate")),
            "water_seal_mm": _mplus_cell(getattr(mapping, "water_seal_mm", ""), _fallback_mplus_mapping_value(article_number, "water_seal_mm")),
            "outlet_dn": _mplus_cell(getattr(mapping, "outlet_dn", ""), _fallback_mplus_mapping_value(article_number, "outlet_dn")),
            "height_adj_min_mm": _fallback_mplus_mapping_value(article_number, "height_adj_min_mm"),
            "height_adj_max_mm": _fallback_mplus_mapping_value(article_number, "height_adj_max_mm"),
            "flow_rate_lps": "",
            "flow_rate_lps_10mm_head": getattr(flow_10mm, "flow_rate_lps", ""),
            "flow_rate_lps_20mm_head": getattr(flow_20mm, "flow_rate_lps", ""),
            "selected_default_flow_rate_lps": policy_report.selected_default_flow_rate_lps,
            "accessory_flow_reduction_lps": accessory_reduction,
            "flow_policy": policy_report.recommended_policy,
            "flow_evidence_type": getattr(flow_evidence, "evidence_type", ""),
            "flow_confidence": getattr(flow_evidence, "confidence", ""),
            "flow_article_specific": bool(getattr(flow_evidence, "article_specific", False)),
            "flow_attribution_scope": getattr(flow_evidence, "flow_attribution_scope", ""),
            "missing_technical_fields": "flow_rate_lps",
            "data_quality_status": "partial",
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": MPLUS_BLOCKING_REASON,
            "recommended_next_action": MPLUS_RECOMMENDED_NEXT_ACTION,
            "production_status_note": MPLUS_PRODUCTION_STATUS_NOTE,
        }
        rows.append(row)

    return pd.DataFrame(rows, columns=MPLUS_COMPOUND_MAPPING_COLUMNS)





def _has_aco_export_context(*dfs: pd.DataFrame) -> bool:
    for df in dfs:
        if df is None or df.empty or "manufacturer" not in df.columns:
            continue
        manufacturers = df.get("manufacturer", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).str.strip().str.lower()
        if manufacturers.eq("aco").any():
            return True
    return False

def _append_mplus_final_assembly_rows(
    products_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    mplus_compound_mappings_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Promote M+ compound mappings into blocked final assembled Product rows.

    Flow-rate values are deliberately not copied into scalar flow_rate_lps.  The
    two condition-specific observations remain in Conditional_Technical_Values.
    """
    products_df = pd.DataFrame() if products_df is None else products_df.copy()
    comparison_df = pd.DataFrame() if comparison_df is None else comparison_df.copy()
    if mplus_compound_mappings_df is None or mplus_compound_mappings_df.empty:
        return products_df, comparison_df

    existing_product_ids = set(products_df["product_id"].fillna("").astype(str).str.strip()) if "product_id" in products_df.columns else set()
    existing_comparison_ids = set(comparison_df["product_id"].fillna("").astype(str).str.strip()) if "product_id" in comparison_df.columns else set()

    product_rows: list[dict[str, Any]] = []
    comparison_rows: list[dict[str, Any]] = []
    for _, mapping in mplus_compound_mappings_df.iterrows():
        channel_body_id = str(mapping.get("channel_body_id") or "").strip()
        drain_body_id = str(mapping.get("drain_body_id") or "").strip()
        grate_id = str(mapping.get("grate_id") or "").strip()
        if not (channel_body_id and drain_body_id and grate_id):
            continue
        assembled_id = _mplus_assembled_product_id(channel_body_id, drain_body_id, grate_id)
        if not assembled_id:
            continue
        article = str(mapping.get("drain_body_article_number") or "").strip()
        name = f"ACO ShowerDrain M+ assembled set {article}".strip()
        product_row = {
            "manufacturer": "aco",
            "product_id": assembled_id,
            "product_name": name,
            "candidate_type": "drain",
            "product_family": "showerdrain_mplus",
            "family": "showerdrain_mplus",
            "complete_system": "yes",
            "system_role": "assembled_system",
            "promote_to_product": "yes",
            "promotion_reason": "assembled_from_mplus_compound_mapping",
            "why_not_product_reason": "",
            "assembly_reason": "aco_mplus_channel_body_x_drain_body_x_grate",
            "assembly_model": "channel_body_x_drain_body_x_grate",
            "assembled_from_bom": True,
            "channel_body_id": mapping.get("channel_body_id", ""),
            "drain_body_id": mapping.get("drain_body_id", ""),
            "grate_id": mapping.get("grate_id", ""),
            "channel_body_article_number": mapping.get("channel_body_article_number", ""),
            "drain_body_article_number": mapping.get("drain_body_article_number", ""),
            "grate_article_number": mapping.get("grate_article_number", ""),
            "source_url_channel_body": mapping.get("source_url_channel_body", ""),
            "source_url_drain_body": mapping.get("source_url_drain_body", ""),
            "source_url_grate": mapping.get("source_url_grate", ""),
            "base_product_id": mapping.get("channel_body_id", ""),
            "drain_body_component_id": mapping.get("drain_body_id", ""),
            "grate_component_id": mapping.get("grate_id", ""),
            "matched_component_ids": ",".join(
                str(mapping.get(k) or "")
                for k in ("channel_body_id", "drain_body_id", "grate_id")
                if str(mapping.get(k) or "").strip()
            ),
            "flow_rate_lps": "",
            "flow_rate_status": MPLUS_FLOW_RATE_STATUS,
            "water_seal_mm": mapping.get("water_seal_mm", ""),
            "outlet_dn": mapping.get("outlet_dn", ""),
            "height_adj_min_mm": mapping.get("height_adj_min_mm", ""),
            "height_adj_max_mm": mapping.get("height_adj_max_mm", ""),
            "is_complete_technical_data": False,
            "missing_technical_fields": "flow_rate_lps",
            "data_quality_status": MPLUS_FINAL_DATA_QUALITY_STATUS,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocked_reason": MPLUS_BLOCKING_REASON,
            "source_status_note": MPLUS_PRODUCTION_STATUS_NOTE,
            "product_url": mapping.get("source_url_channel_body", ""),
            "source_url": mapping.get("source_url_channel_body", ""),
            "sources": ",".join(
                str(mapping.get(k) or "")
                for k in ("source_url_channel_body", "source_url_drain_body", "source_url_grate")
                if str(mapping.get(k) or "").strip()
            ),
        }
        if assembled_id in existing_product_ids and "product_id" in products_df.columns:
            product_mask = products_df["product_id"].fillna("").astype(str).str.strip().eq(assembled_id)
            for column, value in product_row.items():
                if column not in products_df.columns:
                    products_df[column] = ""
                elif isinstance(value, bool):
                    products_df[column] = products_df[column].astype(object)
                products_df.loc[product_mask, column] = value
        else:
            product_rows.append(product_row)
            existing_product_ids.add(assembled_id)

        comparison_row = {
                "manufacturer": "aco",
                "product_id": assembled_id,
                "product_name": name,
                "candidate_type": "drain",
                "product_family": "showerdrain_mplus",
                "family": "showerdrain_mplus",
                "complete_system": "yes",
                "system_role": "assembled_system",
                "promote_to_product": "yes",
                "promotion_reason": "assembled_from_mplus_compound_mapping",
                "why_not_product_reason": "",
                "assembled_from_bom": True,
                "matched_component_ids": product_row["matched_component_ids"],
                "flow_rate_lps": "",
                "flow_rate_status": MPLUS_FLOW_RATE_STATUS,
                "water_seal_mm": mapping.get("water_seal_mm", ""),
                "outlet_dn": mapping.get("outlet_dn", ""),
                "height_adj_min_mm": mapping.get("height_adj_min_mm", ""),
                "height_adj_max_mm": mapping.get("height_adj_max_mm", ""),
                "ready_for_benchmark": False,
                "ready_for_customer_view": False,
                "blocked_reason": MPLUS_BLOCKING_REASON,
                "data_quality_status": MPLUS_FINAL_DATA_QUALITY_STATUS,
            }
        if assembled_id in existing_comparison_ids and "product_id" in comparison_df.columns:
            comparison_mask = comparison_df["product_id"].fillna("").astype(str).str.strip().eq(assembled_id)
            for column, value in comparison_row.items():
                if column not in comparison_df.columns:
                    comparison_df[column] = ""
                elif isinstance(value, bool):
                    comparison_df[column] = comparison_df[column].astype(object)
                comparison_df.loc[comparison_mask, column] = value
        else:
            comparison_rows.append(comparison_row)
            existing_comparison_ids.add(assembled_id)

    if product_rows:
        products_df = pd.concat([products_df, pd.DataFrame(product_rows)], ignore_index=True, sort=False)
    if comparison_rows:
        comparison_df = pd.concat([comparison_df, pd.DataFrame(comparison_rows)], ignore_index=True, sort=False)
    return products_df, comparison_df



def _bool_like(value: Any, default: bool = False) -> bool:
    if value is None or _is_nan(value):
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "ja"}:
        return True
    if text in {"false", "0", "no", "n", "nein"}:
        return False
    return default


def _cplus_assembled_product_id(base_id: Any, grate_id: Any) -> str:
    base = str(base_id or "").strip().lower()
    grate = str(grate_id or "").strip().lower()
    if not base or not grate:
        return ""
    return f"aco-assembled-showerdrain-cplus-{base}__{grate}"


def _append_cplus_production_rows(
    products_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    bom_options_df: pd.DataFrame,
    components_df: pd.DataFrame,
    evidence_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Promote only validated explicit C+ evidence rows into production exports."""
    products_df = pd.DataFrame() if products_df is None else products_df.copy()
    comparison_df = pd.DataFrame() if comparison_df is None else comparison_df.copy()
    bom_options_df = pd.DataFrame() if bom_options_df is None else bom_options_df.copy()
    components_df = pd.DataFrame() if components_df is None else components_df.copy()
    evidence_df = pd.DataFrame() if evidence_df is None else evidence_df.copy()
    required = {
        "product_family", "base_id", "grate_id", "grate_article_number",
        "compatibility_evidence_type", "compatibility_confidence",
        "article_level_compatibility_found", "safe_to_generate",
        "ready_for_benchmark", "ready_for_customer_view",
    }
    if evidence_df.empty or not required.issubset(evidence_df.columns):
        return products_df, comparison_df, bom_options_df

    eligible = evidence_df[
        evidence_df["product_family"].fillna("").astype(str).str.strip().eq("showerdrain_cplus")
        & evidence_df["compatibility_evidence_type"].fillna("").astype(str).str.strip().eq("explicit_catalog_matrix")
        & evidence_df["compatibility_confidence"].fillna("").astype(str).str.strip().eq("high")
        & evidence_df["article_level_compatibility_found"].map(_bool_like)
        & evidence_df["safe_to_generate"].map(_bool_like)
        & evidence_df["ready_for_benchmark"].map(_bool_like)
        & ~evidence_df["ready_for_customer_view"].map(_bool_like)
    ].copy()
    if eligible.empty:
        return products_df, comparison_df, bom_options_df

    product_lookup = {
        str(row.get("product_id") or "").strip(): row
        for _, row in products_df.iterrows()
        if str(row.get("product_id") or "").strip()
    }
    component_lookup = {
        str(row.get("product_id") or "").strip(): row
        for _, row in components_df.iterrows()
        if str(row.get("product_id") or "").strip()
    }
    existing_product_ids = set(product_lookup)
    existing_comparison_ids = set(comparison_df.get("product_id", pd.Series(dtype=str)).fillna("").astype(str))
    existing_bom_keys = {
        (str(row.get("product_id") or "").strip(), str(row.get("component_id") or "").strip(), str(row.get("option_type") or "").strip())
        for _, row in bom_options_df.iterrows()
    }
    product_rows: list[dict[str, Any]] = []
    comparison_rows: list[dict[str, Any]] = []
    bom_rows: list[dict[str, Any]] = []
    seen_assembled_ids: set[str] = set()

    for _, mapping in eligible.sort_values(["base_id", "grate_article_number", "grate_id"]).iterrows():
        base_id = str(mapping.get("base_id") or "").strip()
        grate_id = str(mapping.get("grate_id") or "").strip()
        grate_article = str(mapping.get("grate_article_number") or "").strip()
        if not base_id or not grate_id or base_id == grate_id or grate_id.startswith("aco-assembled-"):
            continue
        if not grate_article or grate_article.lower() == "nan" or grate_article.startswith("9010.85."):
            continue
        base = product_lookup.get(base_id)
        grate = component_lookup.get(grate_id)
        if base is None or grate is None:
            continue
        grate_role = str(grate.get("system_role") or "").strip().lower()
        if grate_role != "grate":
            continue
        assembled_id = _cplus_assembled_product_id(base_id, grate_id)
        if not assembled_id or assembled_id in seen_assembled_ids:
            continue
        seen_assembled_ids.add(assembled_id)

        source_note = (
            "explicit C+ catalog matrix on pages 25-26; protected C+ body and stainless "
            "grate articles are compatible only at equal nominal length"
        )
        sources = " | ".join(filter(None, [
            str(mapping.get("base_source_url") or "").strip(),
            str(mapping.get("grate_source_url") or "").strip(),
        ]))
        name = f"{str(base.get('product_name') or 'ACO ShowerDrain C+').strip()} + {str(grate.get('product_name') or grate_article).strip()}"
        row = dict(base)
        row.update({
            "manufacturer": "aco",
            "product_id": assembled_id,
            "product_name": name,
            "candidate_type": "drain",
            "product_family": "showerdrain_cplus",
            "family": "showerdrain_cplus",
            "complete_system": "yes",
            "system_role": "assembled_system",
            "promote_to_product": "yes",
            "promotion_reason": "assembled_from_cplus_explicit_catalog_matrix",
            "why_not_product_reason": "",
            "assembly_reason": "aco_cplus_explicit_catalog_base_grate_assembly",
            "assembly_model": "base_x_grate",
            "assembled_from_bom": True,
            "base_id": base_id,
            "base_product_id": base_id,
            "base_article_number": mapping.get("base_article_number", ""),
            "base_source_url": mapping.get("base_source_url", ""),
            "grate_id": grate_id,
            "grate_component_id": grate_id,
            "grate_article_number": grate_article,
            "grate_source_url": mapping.get("grate_source_url", ""),
            "matched_component_ids": f"{base_id},{grate_id}",
            "source_text_or_reason": mapping.get("source_text_or_reason", ""),
            "compatibility_evidence_type": "explicit_catalog_matrix",
            "compatibility_confidence": "high",
            "article_level_compatibility_found": True,
            "explicit_article_matrix": True,
            "ready_for_benchmark": True,
            "ready_for_customer_view": False,
            "customer_view_enabled": False,
            "blocked_reason": "",
            "data_quality_status": "explicit_source_ready_production_assembly",
            "source_status_note": source_note,
            "source_url": mapping.get("base_source_url", ""),
            "sources": sources,
        })
        for field in FINAL_ASSEMBLIES_REQUIRED_TECHNICAL_FIELDS:
            row[field] = mapping.get(field, base.get(field, ""))
        if assembled_id not in existing_product_ids:
            product_rows.append(row)
            existing_product_ids.add(assembled_id)
        if assembled_id not in existing_comparison_ids:
            comparison_rows.append(dict(row))
            existing_comparison_ids.add(assembled_id)

        bom_key = (base_id, grate_id, "compatible_grate")
        if bom_key not in existing_bom_keys:
            bom_rows.append({
                "manufacturer": "aco",
                "product_id": base_id,
                "base_id": base_id,
                "component_id": grate_id,
                "option_type": "compatible_grate",
                "option_role": "grate",
                "product_family": "showerdrain_cplus",
                "parent_family": "showerdrain_cplus",
                "option_family": "showerdrain_cplus",
                "component_family": str(grate.get("product_family") or "showerdrain_c_article_grate"),
                "base_article_number": mapping.get("base_article_number", ""),
                "grate_article_number": grate_article,
                "compatibility_evidence_type": "explicit_catalog_matrix",
                "compatibility_confidence": "high",
                "article_level_compatibility_found": True,
                "source_url": mapping.get("grate_source_url", ""),
                "source_text_or_reason": mapping.get("source_text_or_reason", ""),
                "option_meta": "compatibility_confidence=high; explicit_article_matrix=true; equal_nominal_length=true",
            })
            existing_bom_keys.add(bom_key)

    if product_rows:
        products_df = pd.concat([products_df, pd.DataFrame(product_rows)], ignore_index=True, sort=False)
    if comparison_rows:
        comparison_df = pd.concat([comparison_df, pd.DataFrame(comparison_rows)], ignore_index=True, sort=False)
    if bom_rows:
        bom_options_df = pd.concat([bom_options_df, pd.DataFrame(bom_rows)], ignore_index=True, sort=False)
    return products_df, comparison_df, bom_options_df

def _extract_conditional_technical_values(mplus_compound_mappings_df: pd.DataFrame) -> pd.DataFrame:
    """Return diagnostic-only condition-specific technical values for proposal rows.

    M+ flow-rate values are source-backed at different head-water levels.  This
    sheet preserves those variants without selecting a production default or
    feeding the values into Products.flow_rate_lps, scoring, or BOM generation.
    """
    rows: list[dict[str, Any]] = []
    if mplus_compound_mappings_df is None or mplus_compound_mappings_df.empty:
        return pd.DataFrame(columns=CONDITIONAL_TECHNICAL_VALUES_COLUMNS)

    for _, mapping in mplus_compound_mappings_df.iterrows():
        for variant in MPLUS_CONDITIONAL_FLOW_VARIANTS:
            source_column = variant["mplus_source_column"]
            value = pd.to_numeric(_mplus_cell(mapping.get(source_column, ""), variant["value"]), errors="coerce")
            if pd.isna(value):
                value = variant["value"]
            rows.append(
                {
                    "set_id": mapping.get("set_id", ""),
                    "product_family": mapping.get("product_family", ""),
                    "assembly_model": mapping.get("assembly_model", ""),
                    "parameter_name": variant["parameter_name"],
                    "value": value,
                    "unit": variant["unit"],
                    "condition_type": variant["condition_type"],
                    "condition_value": variant["condition_value"],
                    "condition_unit": variant["condition_unit"],
                    "condition_label": variant["condition_label"],
                    "channel_body_id": mapping.get("channel_body_id", ""),
                    "drain_body_id": mapping.get("drain_body_id", ""),
                    "grate_id": mapping.get("grate_id", ""),
                    "source_url_channel_body": mapping.get("source_url_channel_body", ""),
                    "source_url_drain_body": mapping.get("source_url_drain_body", ""),
                    "source_url_grate": mapping.get("source_url_grate", ""),
                    "evidence_type": mapping.get("flow_evidence_type", ""),
                    "confidence": mapping.get("flow_confidence", ""),
                    "attribution_scope": mapping.get("flow_attribution_scope", ""),
                    "article_specific": mapping.get("flow_article_specific", False),
                    "data_quality_status": "conditional_parameter_available_production_blocked",
                    "safe_to_generate": False,
                    "ready_for_benchmark": False,
                    "ready_for_customer_view": False,
                    "blocking_reason": MPLUS_BLOCKING_REASON,
                    "recommended_next_action": MPLUS_RECOMMENDED_NEXT_ACTION,
                    "production_status_note": MPLUS_PRODUCTION_STATUS_NOTE,
                }
            )

    return pd.DataFrame(rows, columns=CONDITIONAL_TECHNICAL_VALUES_COLUMNS)

def export_excel(
    template_path: str,
    out_path: str,
    cfg: Any,
    registry_df: Optional[pd.DataFrame] = None,
    products_df: Optional[pd.DataFrame] = None,
    comparison_df: Optional[pd.DataFrame] = None,
    excluded_df: Optional[pd.DataFrame] = None,
    evidence_df: Optional[pd.DataFrame] = None,
    bom_options_df: Optional[pd.DataFrame] = None,
    components_df: Optional[pd.DataFrame] = None,
) -> None:
    """
    Exportuje výsledky do XLSX.

    Sheets:
    - Candidates_All
    - Products
    - Final_Assemblies
    - Final_Set_Details
    - Components
    - Comparison
    - Excluded
    - Evidence
    - BOM_Options
    - Source_Checks
    - Mplus_Compound_Mappings
    - Eplus_Proposal_Mappings
    - Cplus_Compatible_Grate_Evidence
    - Article_Variants
    - Final_Scoring_Weights
    - Legacy_Equivalence_Weights
    - Config
    """
    wb = openpyxl.load_workbook(template_path)

    registry_df = pd.DataFrame() if registry_df is None else registry_df.copy()
    products_df = pd.DataFrame() if products_df is None else products_df.copy()
    comparison_df = pd.DataFrame() if comparison_df is None else comparison_df.copy()
    excluded_df = pd.DataFrame() if excluded_df is None else excluded_df.copy()
    evidence_df = pd.DataFrame() if evidence_df is None else evidence_df.copy()
    bom_options_df = pd.DataFrame() if bom_options_df is None else bom_options_df.copy()
    components_df = pd.DataFrame() if components_df is None else components_df.copy()

    # Odstranit staré template listy, které už nemají být součástí hlavního scoringu.
    # Legacy equivalence data se exportuje explicitně do Legacy_Equivalence_Weights
    # s enabled=False.
    for obsolete_sheet in ["Equivalence_Weights"]:
        if obsolete_sheet in wb.sheetnames:
            ws_old = wb[obsolete_sheet]
            wb.remove(ws_old)

    # --- AUTO split base_set/component -> Components ---
    if not products_df.empty and components_df.empty:
        df = products_df.copy()

        if "candidate_type" in df.columns:
            cand = df["candidate_type"].astype(str).str.lower()
        else:
            cand = pd.Series([""] * len(df), index=df.index)

        if "complete_system" in df.columns:
            comp = df["complete_system"].astype(str).str.lower()
        else:
            comp = pd.Series([""] * len(df), index=df.index)

        is_component = (
            cand.isin(["base_set", "component"])
            | comp.str.contains("component/base-set", na=False)
            | comp.str.contains("component", na=False)
        )

        components_df = df[is_component].copy()
        products_df = df[~is_component].copy()

    # If components are intentionally excluded from Products (e.g., ACO component-only rows),
    # materialize them into Components from Excluded when no explicit Components were provided.
    if components_df.empty and not excluded_df.empty:
        ex = excluded_df.copy()
        cand = ex.get("candidate_type", pd.Series([""] * len(ex), index=ex.index)).astype(str).str.lower()
        role = ex.get("system_role", pd.Series([""] * len(ex), index=ex.index)).astype(str).str.lower()
        why = ex.get("why_not_product_reason", pd.Series([""] * len(ex), index=ex.index)).astype(str).str.lower()
        status = ex.get("current_status", pd.Series([""] * len(ex), index=ex.index)).astype(str).str.lower()
        is_component = (
            cand.isin(["component", "base_set"])
            | role.isin(["grate", "accessory", "optional_accessory"])
            | why.isin({"cover_only_component", "accessory_only", "component_not_final_product", "configuration_family_not_final_product", "incomplete_assembly"})
            | status.str.contains("component", na=False)
        )
        components_df = ex[is_component].copy()

    def write_df(sheet_name: str, df: pd.DataFrame) -> None:
        # Přepiš sheet, aby v template nezůstávaly staré/hybridní hodnoty.
        if sheet_name in wb.sheetnames:
            ws_old = wb[sheet_name]
            wb.remove(ws_old)

        ws = wb.create_sheet(sheet_name)

        df = pd.DataFrame() if df is None else df.copy()

        # Hlavička
        ws.append([_sanitize_excel_string(str(c)) for c in df.columns])

        # Řádky
        for _, row in df.iterrows():
            ws.append([_to_excel_cell(v) for v in row.tolist()])

    initial_final_assemblies_df = _extract_final_assemblies(products_df)
    initial_final_set_details_df = _extract_final_set_details(initial_final_assemblies_df, bom_options_df, components_df)
    mplus_compound_mappings_df = _extract_mplus_compound_mappings(
        registry_df,
        products_df,
        comparison_df,
        components_df,
        bom_options_df,
        initial_final_assemblies_df,
        initial_final_set_details_df,
    )
    if _has_aco_export_context(registry_df, products_df, comparison_df, components_df, bom_options_df):
        products_df, comparison_df = _append_mplus_final_assembly_rows(
            products_df,
            comparison_df,
            mplus_compound_mappings_df,
        )
    # Imported lazily to avoid coupling the core exporter to the report CLI at module import time.
    from tools.report_cplus_compatible_grate_evidence import build_export_evidence_dataframe

    cplus_compatible_grate_evidence_df = build_export_evidence_dataframe(products_df, components_df)
    products_df, comparison_df, bom_options_df = _append_cplus_production_rows(
        products_df,
        comparison_df,
        bom_options_df,
        components_df,
        cplus_compatible_grate_evidence_df,
    )
    final_assemblies_df = _extract_final_assemblies(products_df)
    final_set_details_df = _extract_final_set_details(final_assemblies_df, bom_options_df, components_df)
    eplus_proposal_mappings_df = _extract_eplus_proposal_mappings(
        products_df,
        final_assemblies_df,
        final_set_details_df,
    )
    conditional_technical_values_df = _extract_conditional_technical_values(mplus_compound_mappings_df)
    scoring_scenarios_df = scoring_scenarios_dataframe()
    comparison_flow_head_10mm_df = build_scenario_comparison(
        comparison_df, conditional_technical_values_df, "flow_head_10mm"
    )
    comparison_flow_head_20mm_df = build_scenario_comparison(
        comparison_df, conditional_technical_values_df, "flow_head_20mm"
    )

    write_df("Candidates_All", registry_df)
    write_df("Products", products_df)
    write_df("Final_Assemblies", final_assemblies_df)
    write_df("Final_Set_Details", final_set_details_df)
    write_df("Mplus_Compound_Mappings", mplus_compound_mappings_df)
    write_df("Eplus_Proposal_Mappings", eplus_proposal_mappings_df)
    write_df("Cplus_Compatible_Grate_Evidence", cplus_compatible_grate_evidence_df)
    write_df("Conditional_Technical_Values", conditional_technical_values_df)
    write_df("Scoring_Scenarios", scoring_scenarios_df)
    write_df("Comparison_flow_head_10mm", comparison_flow_head_10mm_df)
    write_df("Comparison_flow_head_20mm", comparison_flow_head_20mm_df)
    write_df("Components", components_df)
    write_df("Comparison", comparison_df)
    write_df("Excluded", excluded_df)
    write_df("Evidence", evidence_df)
    write_df("BOM_Options", bom_options_df)
    write_df("Source_Checks", _extract_source_checks(evidence_df))
    write_df("Article_Variants", _extract_article_variants(registry_df, products_df))
    write_df("Final_Scoring_Weights", _extract_final_scoring_weights(cfg))
    write_df("Legacy_Equivalence_Weights", _extract_legacy_equivalence_weights(cfg))
    write_df("Config", _extract_config_sheet())
    write_df("Scoring_Field_Coverage", _scoring_field_coverage(products_df, comparison_df))

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        wb.save(out_path)
    finally:
        wb.close()
