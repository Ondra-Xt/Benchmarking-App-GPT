from __future__ import annotations
from typing import Any, Dict, List, Tuple
import hashlib
import json
from datetime import datetime, timezone
import re
import requests

SEEDS = {
    "flow": "https://www.kaldewei.com/products/kaldewei-flow/",
    "nexsys": "https://www.kaldewei.com/products/showers/detail/product/nexsys/",
    "waste": "https://www.kaldewei.com/products/showers/shower-accessories/waste-systems/",
    "conoflat": "https://www.kaldewei.com/products/showers/detail/product/conoflat/",
    "calima": "https://www.kaldewei.com/products/calima/",
    "xetis": "https://www.kaldewei.com/products/showers/detail/product/xetis/",
}
SOURCE_REGISTRY: List[Dict[str, Any]] = [
    {"source_id": "kaldewei-flow-page", "family": "flow", "source_url": SEEDS["flow"], "source_type": "product_page", "critical_expected_terms": ["FLOWLINE ZERO", "FLOWPOINT ZERO", "FLOWDRAIN"], "warning_expected_terms": ["brushed steel", "brushed champagne", "brushed graphite", "alpine white matt", "black matt 100"], "criticality": "high", "review_area": "flow"},
    {"source_id": "kaldewei-flowdrain-horizontal-pdf", "family": "flowdrain", "source_url": "https://files.cdn.kaldewei.com/data/sprachen/englisch/prospekte/installationsanleitungen/DB24_GB_DA_ZUB_FLOWDRAIN_HORIZONTAL_WEB.pdf", "source_type": "pdf", "critical_expected_terms": ["FLOWDRAIN", "DN 50", "DN 40", "0.8", "0.63", "50 mm", "30 mm"], "criticality": "high", "review_area": "flowdrain"},
    {"source_id": "kaldewei-nexsys-product-page", "family": "nexsys", "source_url": SEEDS["nexsys"], "source_type": "product_page", "critical_expected_terms": ["NEXSYS", "design cover", "4-in-1"], "criticality": "high", "review_area": "nexsys"},
    {"source_id": "kaldewei-nexsys-ka-4121-4122-pdf", "family": "nexsys", "source_url": "https://files.cdn.kaldewei.com/data/sprachen/tschechisch/techdata/DB03_21-Export_Print-CZcz_ST_AC_KA4121_KA4122_NEXSYS.pdf", "source_type": "pdf", "critical_expected_terms": ["KA 4121", "KA 4122", "NEXSYS"], "criticality": "high", "review_area": "nexsys"},
    {"source_id": "kaldewei-waste-systems-page", "family": "waste", "source_url": SEEDS["waste"], "source_type": "product_page", "critical_expected_terms": ["KA 90", "KA 120", "KA 300"], "criticality": "medium", "review_area": "waste"},
    {"source_id": "kaldewei-calima-ka-300-page", "family": "ka_300", "source_url": SEEDS["calima"], "source_type": "product_page", "critical_expected_terms": ["CALIMA", "KA 300"], "criticality": "medium", "review_area": "ka_300"},
    {"source_id": "kaldewei-conoflat-ka-120-techdata", "family": "ka_120", "source_url": SEEDS["conoflat"], "source_type": "product_page", "critical_expected_terms": ["CONOFLAT"], "warning_expected_terms": ["KA 120"], "criticality": "medium", "review_area": "ka_120"},
    {"source_id": "kaldewei-ka-120-ka-125-legacy-sheet", "family": "ka_125", "source_url": "https://files.cdn.kaldewei.com/data/sprachen/deutsch/techdata/KADBD_ABLAUFGARNITUR_KA120_und_KA125.pdf", "source_type": "pdf", "critical_expected_terms": ["KA 120", "KA 125"], "criticality": "medium", "review_area": "legacy"},
    {"source_id": "kaldewei-xetis-ka-200-installation-sheet", "family": "xetis", "source_url": "https://files.cdn.kaldewei.com/data/sprachen/deutsch/techdata/DB03_21-Export_Print-DEde_DW_ZB_XETIS_Installation.pdf", "source_type": "pdf", "critical_expected_terms": ["XETIS", "KA 200"], "criticality": "medium", "review_area": "xetis"},
]


BASELINE_PATH = "data/source_baselines/kaldewei_sources.json"

CATALOG: List[Dict[str, Any]] = [
    {"product_id": "kaldewei-nexsys", "product_name": "KALDEWEI NEXSYS", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "drain", "system_role": "complete_system", "complete_system": "yes", "promotion_reason": "integrated_shower_surface_system", "height_adj_min_mm": 84, "cover_lengths_mm": "750,800,900,1000,1200", "shower_sizes_mm": "800x800..900x1700", "system_meta": "integrated_4in1"},
    {"product_id": "kaldewei-flowline-zero", "product_name": "KALDEWEI FLOWLINE ZERO", "product_url": SEEDS["flow"], "family": "flowline_zero", "candidate_type": "component", "system_role": "visible_linear_profile", "complete_system": "component", "available_lengths_mm": "900,1200,1500"},
    {"product_id": "kaldewei-flowpoint-zero", "product_name": "KALDEWEI FLOWPOINT ZERO", "product_url": SEEDS["flow"], "family": "flowpoint_zero", "candidate_type": "component", "system_role": "visible_point_cover", "complete_system": "component"},
    {"product_id": "kaldewei-flowline-zero-finish-brushed-steel", "product_name": "KALDEWEI FLOWLINE ZERO finish brushed steel", "product_url": SEEDS["flow"], "family": "flowline_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "brushed steel", "finish_code": "930", "current_status": "current"},
    {"product_id": "kaldewei-flowline-zero-finish-brushed-champagne", "product_name": "KALDEWEI FLOWLINE ZERO finish brushed champagne", "product_url": SEEDS["flow"], "family": "flowline_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "brushed champagne", "finish_code": "931", "current_status": "current"},
    {"product_id": "kaldewei-flowline-zero-finish-brushed-graphite", "product_name": "KALDEWEI FLOWLINE ZERO finish brushed graphite", "product_url": SEEDS["flow"], "family": "flowline_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "brushed graphite", "finish_code": "932", "current_status": "current"},
    {"product_id": "kaldewei-flowline-zero-finish-alpine-white-matt", "product_name": "KALDEWEI FLOWLINE ZERO finish alpine white matt", "product_url": SEEDS["flow"], "family": "flowline_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "alpine white matt", "finish_code": "711", "current_status": "current"},
    {"product_id": "kaldewei-flowline-zero-finish-black-matt-100", "product_name": "KALDEWEI FLOWLINE ZERO finish black matt 100", "product_url": SEEDS["flow"], "family": "flowline_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "black matt 100", "finish_code": "676", "current_status": "current"},
    {"product_id": "kaldewei-flowpoint-zero-finish-brushed-steel", "product_name": "KALDEWEI FLOWPOINT ZERO finish brushed steel", "product_url": SEEDS["flow"], "family": "flowpoint_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "brushed steel", "finish_code": "930", "current_status": "current"},
    {"product_id": "kaldewei-flowpoint-zero-finish-brushed-champagne", "product_name": "KALDEWEI FLOWPOINT ZERO finish brushed champagne", "product_url": SEEDS["flow"], "family": "flowpoint_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "brushed champagne", "finish_code": "931", "current_status": "current"},
    {"product_id": "kaldewei-flowpoint-zero-finish-brushed-graphite", "product_name": "KALDEWEI FLOWPOINT ZERO finish brushed graphite", "product_url": SEEDS["flow"], "family": "flowpoint_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "brushed graphite", "finish_code": "932", "current_status": "current"},
    {"product_id": "kaldewei-flowpoint-zero-finish-alpine-white-matt", "product_name": "KALDEWEI FLOWPOINT ZERO finish alpine white matt", "product_url": SEEDS["flow"], "family": "flowpoint_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "alpine white matt", "finish_code": "711", "current_status": "current"},
    {"product_id": "kaldewei-flowpoint-zero-finish-black-matt-100", "product_name": "KALDEWEI FLOWPOINT ZERO finish black matt 100", "product_url": SEEDS["flow"], "family": "flowpoint_zero", "candidate_type": "component", "system_role": "finish_cover", "complete_system": "component", "promotion_reason": "cover_only_component", "finish_name": "black matt 100", "finish_code": "676", "current_status": "current"},
    {"product_id": "kaldewei-flowdrain-horizontal-regular", "product_name": "KALDEWEI FLOWDRAIN horizontal regular", "product_url": SEEDS["waste"], "family": "flowdrain", "candidate_type": "component", "system_role": "trap_set", "complete_system": "component", "outlet_dn": "DN50", "flow_rate_lps": 0.8, "height_adj_min_mm": 78, "height_adj_max_mm": 179, "water_seal_mm": 50},
    {"product_id": "kaldewei-flowdrain-horizontal-flat", "product_name": "KALDEWEI FLOWDRAIN horizontal flat", "product_url": SEEDS["waste"], "family": "flowdrain", "candidate_type": "component", "system_role": "trap_set", "complete_system": "component", "outlet_dn": "DN40", "flow_rate_lps": 0.63, "height_adj_min_mm": 58, "height_adj_max_mm": 78, "water_seal_mm": 30},
    {"product_id": "kaldewei-ka-90-horizontal", "product_name": "KALDEWEI KA 90 horizontal", "product_url": SEEDS["waste"], "family": "ka_90", "candidate_type": "component", "product_category": "tray_waste_fitting", "system_role": "tray_waste_fitting", "complete_system": "component", "benchmark_eligible": False, "model_number": "4103", "article_number": "687772560999", "outlet_orientation": "horizontal", "outlet_dn": "DN50", "dn": "DN50", "flow_rate_lps": 0.71, "water_seal_mm": 50, "construction_height_mm": 80},
    {"product_id": "kaldewei-ka-90-flat", "product_name": "KALDEWEI KA 90 flat", "product_url": SEEDS["waste"], "family": "ka_90", "candidate_type": "component", "product_category": "tray_waste_fitting", "system_role": "tray_waste_fitting", "complete_system": "component", "benchmark_eligible": False, "model_number": "4104", "article_number": "687772540999", "outlet_orientation": "flat", "outlet_dn": "DN40", "dn": "DN40", "flow_rate_lps": 0.68, "water_seal_mm": 30, "construction_height_mm": 60},
    {"product_id": "kaldewei-ka-90-vertical", "product_name": "KALDEWEI KA 90 vertical", "product_url": SEEDS["waste"], "family": "ka_90", "candidate_type": "component", "product_category": "tray_waste_fitting", "system_role": "tray_waste_fitting", "complete_system": "component", "benchmark_eligible": False, "model_number": "4105", "article_number": "687772550999", "outlet_orientation": "vertical", "outlet_dn": "DN50", "dn": "DN50", "flow_rate_lps": 1.22, "water_seal_mm": 50, "construction_height_mm": 80},
    {"product_id": "kaldewei-ka-120-horizontal", "product_name": "KALDEWEI KA 120 horizontal", "product_url": SEEDS["conoflat"], "family": "ka_120", "candidate_type": "component", "product_category": "tray_waste_fitting", "system_role": "tray_waste_fitting", "complete_system": "component", "model_number": "4106", "article_number": "687772530000", "outlet_orientation": "horizontal", "outlet_dn": "DN50", "dn": "DN50", "flow_rate_lps": 0.85, "water_seal_mm": 50, "height_adj_min_mm": 83, "height_adj_max_mm": 83, "construction_height_mm": 83, "current_status": "current", "compatibility_caution": "superplan_plus_unclear"},
    {"product_id": "kaldewei-ka-120-flat", "product_name": "KALDEWEI KA 120 flat", "product_url": SEEDS["conoflat"], "family": "ka_120", "candidate_type": "component", "product_category": "tray_waste_fitting", "system_role": "tray_waste_fitting", "complete_system": "component", "model_number": "4107", "article_number": "687772510000", "outlet_orientation": "flat", "outlet_dn": "DN40", "dn": "DN40", "flow_rate_lps": 0.85, "water_seal_mm": 30, "height_adj_min_mm": 63, "height_adj_max_mm": 63, "construction_height_mm": 63, "current_status": "current", "compatibility_caution": "superplan_plus_unclear"},
    {"product_id": "kaldewei-ka-120-vertical", "product_name": "KALDEWEI KA 120 vertical", "product_url": SEEDS["conoflat"], "family": "ka_120", "candidate_type": "component", "product_category": "tray_waste_fitting", "system_role": "tray_waste_fitting", "complete_system": "component", "model_number": "4108", "article_number": "687772520000", "outlet_orientation": "vertical", "outlet_dn": "DN50", "dn": "DN50", "flow_rate_lps": 1.4, "water_seal_mm": 50, "height_adj_min_mm": 83, "height_adj_max_mm": 83, "construction_height_mm": 83, "current_status": "current", "compatibility_caution": "superplan_plus_unclear"},
    {"product_id": "kaldewei-ka-300-horizontal", "product_name": "KALDEWEI KA 300 horizontal", "product_url": SEEDS["calima"], "family": "ka_300", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "flow_rate_lps": 0.61, "height_adj_min_mm": 112, "height_adj_max_mm": 112},
    {"product_id": "kaldewei-ka-300-flat", "product_name": "KALDEWEI KA 300 flat", "product_url": SEEDS["calima"], "family": "ka_300", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "flow_rate_lps": 0.57, "height_adj_min_mm": 92, "height_adj_max_mm": 92},
    {"product_id": "kaldewei-ka-4121", "product_name": "KALDEWEI KA 4121 NEXSYS drain set", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "product_category": "nexsys_drain_set", "system_role": "drain_set", "complete_system": "component", "benchmark_eligible": False, "promotion_reason": "drain_set_component", "model_number": "4121", "article_number": "687771210000", "flow_rate_lps": 0.72, "flow_rate_10mm_lps": 0.68, "flow_rate_20mm_lps": 0.72, "water_seal_mm": 50, "material_detail": "plastic", "din_en_1253": "yes", "certification_din_en_1253": "DIN EN 1253", "certifications": "DIN EN 1253", "current_status": "current", "compatibility_caution": "nexsys_only", "source_note": "KA4121 official NEXSYS technical data: order no. 687771210000, DIN EN 1253, drainage capacity 0.68/0.72 l/s at 10/20 mm, water seal 50 mm."},
    {"product_id": "kaldewei-ka-4122", "product_name": "KALDEWEI KA 4122 NEXSYS drain set", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "product_category": "nexsys_drain_set", "system_role": "drain_set", "complete_system": "component", "benchmark_eligible": False, "promotion_reason": "drain_set_component", "model_number": "4122", "article_number": "687771220000", "flow_rate_lps": 0.55, "flow_rate_10mm_lps": 0.48, "flow_rate_20mm_lps": 0.55, "water_seal_mm": 25, "material_detail": "plastic", "din_en_1253": "yes", "certification_din_en_1253": "DIN EN 1253", "certifications": "DIN EN 1253", "current_status": "current", "compatibility_caution": "nexsys_only_ultraflat", "source_note": "KA4122 official NEXSYS technical data: order no. 687771220000, DIN EN 1253, drainage capacity 0.48/0.55 l/s at 10/20 mm; retail data confirms 25 mm water seal."},
    {"product_id": "kaldewei-nexsys-design-cover-brushed", "product_name": "KALDEWEI NEXSYS design cover brushed", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "system_role": "design_cover", "complete_system": "component", "promotion_reason": "cover_only_component"},
    {"product_id": "kaldewei-nexsys-design-cover-polished", "product_name": "KALDEWEI NEXSYS design cover polished", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "system_role": "design_cover", "complete_system": "component", "promotion_reason": "cover_only_component"},
    {"product_id": "kaldewei-nexsys-design-cover-coated-white", "product_name": "KALDEWEI NEXSYS design cover coated white", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "system_role": "design_cover", "complete_system": "component", "promotion_reason": "cover_only_component"},
    {"product_id": "kaldewei-ka-125-legacy", "product_name": "KALDEWEI KA 125", "product_url": SEEDS["waste"], "family": "ka_125", "candidate_type": "component", "system_role": "waste_fitting_legacy", "complete_system": "component", "current_status": "legacy_or_current_unclear"},
    {"product_id": "kaldewei-xetis-ka-200", "product_name": "KALDEWEI XETIS / KA 200", "product_url": SEEDS["xetis"], "family": "xetis", "candidate_type": "drain", "system_role": "complete_system", "complete_system": "configuration", "current_status": "current", "promotion_reason": "xetis_configuration_with_ka200", "flow_rate_lps": 1.0, "water_seal_mm": 50, "material_detail": "stainless_steel", "din_en_274": "yes", "certification_din_en_274": "DIN EN 274", "certifications": "DIN EN 274", "source_note": "Official XETIS / KA200 technical data: drainage capacity 1.0 l/s according to DIN EN 274."},
    {"product_id": "kaldewei-ka-200", "product_name": "KALDEWEI KA 200 XETIS drain set", "product_url": SEEDS["xetis"], "family": "xetis", "candidate_type": "component", "product_category": "xetis_drain_set", "system_role": "drain_set", "complete_system": "component", "benchmark_eligible": False, "promotion_reason": "drain_set_component", "model_number": "KA 200", "article_number": "687676270000", "flow_rate_lps": 1.0, "water_seal_mm": 50, "material_detail": "stainless_steel", "din_en_274": "yes", "certification_din_en_274": "DIN EN 274", "certifications": "DIN EN 274", "current_status": "current", "compatibility_caution": "xetis_only", "source_note": "Official XETIS / KA200 technical data: KA200, DIN EN 274, drainage capacity 1.0 l/s."},
    {"product_id": "kaldewei-xetis-installation-set-1", "product_name": "KALDEWEI XETIS installation set 1", "product_url": SEEDS["xetis"], "family": "xetis", "candidate_type": "component", "system_role": "installation_set", "complete_system": "component", "benchmark_eligible": False, "article_number": "687676310000", "current_status": "current"},
    {"product_id": "kaldewei-xetis-installation-set-2", "product_name": "KALDEWEI XETIS installation set 2", "product_url": SEEDS["xetis"], "family": "xetis", "candidate_type": "component", "system_role": "installation_set", "complete_system": "component", "benchmark_eligible": False, "article_number": "687676320000", "current_status": "current"},
    {"product_id": "kaldewei-xetis-installation-set-3", "product_name": "KALDEWEI XETIS installation set 3", "product_url": SEEDS["xetis"], "family": "xetis", "candidate_type": "component", "system_role": "installation_set", "complete_system": "component", "benchmark_eligible": False, "article_number": "687676390000", "current_status": "current"},
]

BOM = {
    "kaldewei-flowline-zero": [("kaldewei-flowdrain-horizontal-regular", "required_trap_set"), ("kaldewei-flowdrain-horizontal-flat", "required_trap_set")],
    "kaldewei-flowpoint-zero": [("kaldewei-flowdrain-horizontal-regular", "required_trap_set"), ("kaldewei-flowdrain-horizontal-flat", "required_trap_set")],
    "kaldewei-nexsys": [("kaldewei-ka-4121", "compatible_drain_set"), ("kaldewei-ka-4122", "compatible_drain_set"), ("kaldewei-nexsys-design-cover-brushed", "compatible_cover"), ("kaldewei-nexsys-design-cover-polished", "compatible_cover"), ("kaldewei-nexsys-design-cover-coated-white", "compatible_cover")],
    "kaldewei-xetis-ka-200": [("kaldewei-ka-200", "compatible_drain_set"), ("kaldewei-xetis-installation-set-1", "installation_option"), ("kaldewei-xetis-installation-set-2", "installation_option"), ("kaldewei-xetis-installation-set-3", "installation_option")],
}
BOM["kaldewei-flowline-zero"].extend([
    ("kaldewei-flowline-zero-finish-brushed-steel", "compatible_finish"),
    ("kaldewei-flowline-zero-finish-brushed-champagne", "compatible_finish"),
    ("kaldewei-flowline-zero-finish-brushed-graphite", "compatible_finish"),
    ("kaldewei-flowline-zero-finish-alpine-white-matt", "compatible_finish"),
    ("kaldewei-flowline-zero-finish-black-matt-100", "compatible_finish"),
])
BOM["kaldewei-flowpoint-zero"].extend([
    ("kaldewei-flowpoint-zero-finish-brushed-steel", "compatible_finish"),
    ("kaldewei-flowpoint-zero-finish-brushed-champagne", "compatible_finish"),
    ("kaldewei-flowpoint-zero-finish-brushed-graphite", "compatible_finish"),
    ("kaldewei-flowpoint-zero-finish-alpine-white-matt", "compatible_finish"),
    ("kaldewei-flowpoint-zero-finish-black-matt-100", "compatible_finish"),
])


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows = []
    for r in CATALOG:
        row = dict(r)
        row["manufacturer"] = "kaldewei"
        row["product_family"] = str(row.get("family") or "unknown")
        base_url = str(row.get("product_url") or "")
        row["product_url"] = f"{base_url}#{row.get('product_id')}"
        if str(row.get("product_id") or "").startswith("kaldewei-flowline-zero") and "finish" not in str(row.get("product_id") or ""):
            row.setdefault("selected_length_mm", target_length_mm)
        else:
            row["selected_length_mm"] = "not_applicable"
        rows.append(row)
    debug = [{"site": "kaldewei", "method": "seed_catalog", "candidates_found": len(rows), "seed_count": len(SEEDS)}]
    return rows, debug


def extract_parameters(url: str) -> Dict[str, Any]:
    u = (url or "").lower()
    frag = u.split("#")[-1] if "#" in u else ""
    if frag:
        row = next((r for r in CATALOG if str(r.get("product_id") or "").lower() == frag), None)
        if row:
            return {k: v for k, v in row.items() if k in {"flow_rate_lps", "flow_rate_10mm_lps", "flow_rate_20mm_lps", "outlet_dn", "dn", "outlet_orientation", "height_adj_min_mm", "height_adj_max_mm", "construction_height_mm", "water_seal_mm", "current_status", "compatibility_caution", "finish_name", "finish_code", "model_number", "article_number", "product_category", "system_role", "complete_system", "benchmark_eligible", "material_detail", "din_en_1253", "certification_din_en_1253", "din_en_274", "certification_din_en_274", "certifications", "source_note"}}
    for r in CATALOG:
        if str(r.get("product_url") or "").lower() == u:
            return {k: v for k, v in r.items() if k in {"flow_rate_lps", "flow_rate_10mm_lps", "flow_rate_20mm_lps", "outlet_dn", "dn", "outlet_orientation", "height_adj_min_mm", "height_adj_max_mm", "construction_height_mm", "water_seal_mm", "current_status", "compatibility_caution", "finish_name", "finish_code", "model_number", "article_number", "product_category", "system_role", "complete_system", "benchmark_eligible", "material_detail", "din_en_1253", "certification_din_en_1253", "din_en_274", "certification_din_en_274", "certifications", "source_note"}}
    return {}


def get_bom_options(url: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    u = (url or "").lower()
    frag = u.split("#")[-1] if "#" in u else ""
    parent = next((r for r in CATALOG if str(r.get("product_id") or "").lower() == frag), None) if frag else None
    if not parent:
        parent = next((r for r in CATALOG if str(r.get("product_url") or "").lower() == u), None)
    if not parent:
        return []
    pid = str(parent.get("product_id") or "")
    out = []
    for cid, opt_type in BOM.get(pid, []):
        comp = next((r for r in CATALOG if r.get("product_id") == cid), {})
        out.append({
            "component_id": cid,
            "option_type": opt_type,
            "option_label": str(comp.get("product_name") or cid),
            "option_sku": str(comp.get("finish_code") or ""),
            "option_family": str(comp.get("family") or ""),
            "option_role": str(comp.get("system_role") or "component"),
            "parent_family": str(parent.get("family") or ""),
            "source_url": str(parent.get("product_url") or ""),
            "option_meta": f"{pid}:{opt_type}:{str(comp.get('finish_name') or comp.get('product_id') or '')}",
        })
    return out


def _fetch_source(url: str, timeout: int = 20) -> Dict[str, Any]:
    try:
        r = requests.get(url, timeout=timeout, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        content = r.content or b""
        ctype = str(r.headers.get("Content-Type") or "")
        mode = "binary_hash_only" if "pdf" in ctype.lower() or url.lower().endswith(".pdf") else "html_text"
        text = ""
        if mode == "html_text":
            try:
                text = content.decode(r.encoding or "utf-8", errors="ignore")
            except Exception:
                text = ""
        return {"status_code": r.status_code, "final_url": str(r.url), "content": content, "text": text, "content_type": ctype, "mode": mode}
    except Exception as e:
        return {"status_code": None, "final_url": url, "content": b"", "text": "", "content_type": "", "mode": "error", "error": f"{e.__class__.__name__}: {e}"}



def write_kaldewei_source_baseline(source_checks: List[Dict[str, Any]], path: str = BASELINE_PATH) -> str:
    out = []
    for row in source_checks:
        out.append({
            "source_id": row.get("source_id", ""),
            "baseline_hash_sha256": row.get("content_hash_sha256", ""),
            "baseline_content_length": row.get("content_length", ""),
            "source_url": row.get("source_url", ""),
            "source_type": row.get("source_type", ""),
            "written_at": datetime.now(timezone.utc).isoformat(),
        })
    from pathlib import Path as _P
    p = _P(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    return str(p)

def _canonicalize_kaldewei_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("/"):
        u = "https://www.kaldewei.com" + u
    u = u.split('#')[0].split('?')[0].rstrip('/')
    u = u.replace('/en/', '/').replace('/de/', '/').replace('/cz/', '/')
    return u

def _classify_source_candidate(url: str) -> str:
    u = _canonicalize_kaldewei_url(url).lower()
    in_scope = any(k in u for k in ('flow','flowline','flowpoint','flowdrain','nexsys','ka-90','ka-120','ka-125','ka-300','ka','xetis','ka-200','waste-systems','calima','conoflat'))
    if any(d in u for d in ('kaldewei.de','kaldewei.co.uk','kaldewei.es','kaldewei.cz','kaldewei.cn')):
        return 'ignored_language_variant'
    if not u or u.endswith(('.jpg','.jpeg','.png','.webp','.svg','.css','.js','.ico','.gif')) or 'images.cdn.kaldewei.com' in u:
        return 'ignored_asset'
    if 'pricelist.kaldewei.com' in u:
        return 'ignored_pricelist_candidate'
    if 'files.cdn.kaldewei.com' in u and any(k in u for k in ('.pdf','.zip','techdata','datasheet','flyer','installation','prospekte')) and in_scope:
        return 'high_review_candidate'
    if ('kaldewei.com/products/' in u or '/detail/product/' in u) and in_scope:
        if any(k in u for k in ('/showers/','/products/kaldewei-flow','/waste-systems/','/detail/product/')):
            return 'high_review_candidate'
    return 'low_noise_candidate'

def validate_kaldewei_sources(baseline_path: str = BASELINE_PATH) -> List[Dict[str, Any]]:
    try:
        with open(baseline_path, "r", encoding="utf-8") as f:
            baseline = {x["source_id"]: x for x in json.load(f)}
    except Exception:
        baseline = {}
    known_urls = {_canonicalize_kaldewei_url(x["source_url"]) for x in SOURCE_REGISTRY}
    rows: List[Dict[str, Any]] = []
    for src in SOURCE_REGISTRY:
        fetched = _fetch_source(src["source_url"])
        content = fetched.get("content") or b""
        text = (fetched.get("text") or "")
        h = hashlib.sha256(content).hexdigest() if content else ""
        ln = len(content)
        base = baseline.get(src["source_id"], {})
        critical_terms = src.get("critical_expected_terms") or src.get("expected_terms") or []
        warning_terms = src.get("warning_expected_terms") or []
        critical_missing = [t for t in critical_terms if t.lower() not in text.lower()] if fetched.get("mode") == "html_text" else []
        warning_missing = [t for t in warning_terms if t.lower() not in text.lower()] if fetched.get("mode") == "html_text" else []
        candidate_high_med = []
        ignored_candidates = []
        ignored_counts = {"ignored_language_variant":0,"ignored_pricelist_candidate":0,"ignored_asset":0}
        if fetched.get("mode") == "html_text":
            for m in re.findall(r'href=["\']([^"\']+)["\']', text, flags=re.IGNORECASE):
                u = (m or "").strip()
                if not u or u.startswith("#"):
                    continue
                u = _canonicalize_kaldewei_url(u)
                if "kaldewei" not in u.lower():
                    continue
                ctype = _classify_source_candidate(u)
                if u in known_urls:
                    continue
                if ctype in {"high_review_candidate", "medium_review_candidate"}:
                    candidate_high_med.append((u, ctype))
                else:
                    ignored_candidates.append(u)
                    if ctype in ignored_counts:
                        ignored_counts[ctype] += 1
        uniq = []
        seen = set()
        for u, t in candidate_high_med:
            if u in seen:
                continue
            seen.add(u)
            uniq.append((u, t))
        review_reasons = []
        if fetched.get("mode") == "error":
            review_reasons.append("fetch_error")
        elif fetched.get("status_code") != 200:
            review_reasons.append("unreachable_or_non_200")
        if base and base.get("baseline_hash_sha256") and h and base.get("baseline_hash_sha256") != h:
            review_reasons.append("hash_changed")
        if base and base.get("baseline_content_length") not in (None, "") and ln and int(base.get("baseline_content_length")) != ln:
            review_reasons.append("length_changed")
        if critical_missing:
            review_reasons.append("expected_terms_missing")
        if not base:
            review_reasons.append("baseline_missing")
        if uniq:
            review_reasons.append("new_source_candidates")
        product_candidates = [u for u,_ in uniq if "kaldewei.com/products" in u.lower() or "/detail/product/" in u.lower()]
        pdf_candidates = [u for u,_ in uniq if "files.cdn.kaldewei.com" in u.lower()]
        new_product_count = len(product_candidates)
        new_pdf_count = len(pdf_candidates)
        rows.append({"manufacturer":"kaldewei","source_id":src["source_id"],"family":src["family"],"source_url":src["source_url"],"source_type":src["source_type"],"status_code":fetched.get("status_code"),"final_url":fetched.get("final_url"),"content_hash_sha256":h,"content_length":ln,"baseline_hash_sha256":base.get("baseline_hash_sha256", ""),"baseline_content_length":base.get("baseline_content_length", ""),"hash_changed":bool(base and base.get("baseline_hash_sha256") and base.get("baseline_hash_sha256") != h),"length_changed":bool(base and base.get("baseline_content_length") not in (None, "") and ln and int(base.get("baseline_content_length")) != ln),"expected_terms_found":",".join([t for t in critical_terms if t not in critical_missing]),"expected_terms_missing":",".join(critical_missing),"warning_terms_missing":",".join(warning_missing),"new_source_candidate_count":len(uniq),"sample_new_source_candidates":",".join([u for u,_ in uniq[:5]])[:900],"new_source_candidate_types":",".join([t for _,t in uniq]),"new_product_source_candidate_count":new_product_count,"new_pdf_source_candidate_count":new_pdf_count,"sample_new_product_source_candidates":",".join(product_candidates[:5])[:900],"sample_new_pdf_source_candidates":",".join(pdf_candidates[:5])[:900],"ignored_candidate_count":len(set(ignored_candidates)),"sample_ignored_candidates":",".join(sorted(set(ignored_candidates))[:5])[:900],"ignored_language_variant_candidates_count":ignored_counts["ignored_language_variant"],"ignored_pricelist_candidates_count":ignored_counts["ignored_pricelist_candidate"],"ignored_asset_candidates_count":ignored_counts["ignored_asset"],"review_required":"yes" if review_reasons else "no","review_warning":"warning_terms_missing" if warning_missing else "","review_reason":",".join(review_reasons),"checked_at":datetime.now(timezone.utc).isoformat(),"extraction_mode":fetched.get("mode"),"baseline_status":"missing" if not base else "present","fetch_error":str(fetched.get("error") or "")})
    return rows
