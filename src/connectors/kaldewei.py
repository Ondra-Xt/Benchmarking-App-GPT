from __future__ import annotations
from typing import Any, Dict, List, Tuple

SEEDS = {
    "flow": "https://www.kaldewei.com/products/shower-surfaces/kaldewei-flow/",
    "nexsys": "https://www.kaldewei.com/products/shower-surfaces/nexsys/",
    "waste": "https://www.kaldewei.com/products/accessories/waste-fittings/",
    "conoflat": "https://www.kaldewei.com/products/shower-surfaces/conoflat/",
    "calima": "https://www.kaldewei.com/products/shower-surfaces/calima/",
    "xetis": "https://www.kaldewei.com/products/shower-surfaces/xetis/",
}

CATALOG: List[Dict[str, Any]] = [
    {"product_id": "kaldewei-nexsys", "product_name": "KALDEWEI NEXSYS", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "drain", "system_role": "complete_system", "complete_system": "yes", "promotion_reason": "integrated_shower_surface_system", "height_adj_min_mm": 84, "cover_lengths_mm": "750,800,900,1000,1200", "shower_sizes_mm": "800x800..900x1700", "system_meta": "integrated_4in1"},
    {"product_id": "kaldewei-flowline-zero", "product_name": "KALDEWEI FLOWLINE ZERO", "product_url": SEEDS["flow"], "family": "flowline_zero", "candidate_type": "component", "system_role": "visible_linear_profile", "complete_system": "component", "available_lengths_mm": "900,1200,1500"},
    {"product_id": "kaldewei-flowpoint-zero", "product_name": "KALDEWEI FLOWPOINT ZERO", "product_url": SEEDS["flow"], "family": "flowpoint_zero", "candidate_type": "component", "system_role": "visible_point_cover", "complete_system": "component"},
    {"product_id": "kaldewei-flowdrain-horizontal-regular", "product_name": "KALDEWEI FLOWDRAIN horizontal regular", "product_url": SEEDS["waste"], "family": "flowdrain", "candidate_type": "component", "system_role": "trap_set", "complete_system": "component", "outlet_dn": "DN50", "flow_rate_lps": 0.8, "height_adj_min_mm": 78, "height_adj_max_mm": 179, "water_seal_mm": 50},
    {"product_id": "kaldewei-flowdrain-horizontal-flat", "product_name": "KALDEWEI FLOWDRAIN horizontal flat", "product_url": SEEDS["waste"], "family": "flowdrain", "candidate_type": "component", "system_role": "trap_set", "complete_system": "component", "outlet_dn": "DN40", "flow_rate_lps": 0.63, "height_adj_min_mm": 58, "height_adj_max_mm": 78, "water_seal_mm": 30},
    {"product_id": "kaldewei-ka-90-horizontal", "product_name": "KALDEWEI KA 90 horizontal", "product_url": SEEDS["waste"], "family": "ka_90", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "flow_rate_lps": 0.71, "height_adj_min_mm": 80, "height_adj_max_mm": 80},
    {"product_id": "kaldewei-ka-90-flat", "product_name": "KALDEWEI KA 90 flat", "product_url": SEEDS["waste"], "family": "ka_90", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "flow_rate_lps": 0.68, "height_adj_min_mm": 60, "height_adj_max_mm": 60},
    {"product_id": "kaldewei-ka-90-vertical", "product_name": "KALDEWEI KA 90 vertical", "product_url": SEEDS["waste"], "family": "ka_90", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "flow_rate_lps": 1.22, "height_adj_min_mm": 80, "height_adj_max_mm": 80},
    {"product_id": "kaldewei-ka-120-horizontal", "product_name": "KALDEWEI KA 120 horizontal", "product_url": SEEDS["conoflat"], "family": "ka_120", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "current_status": "current", "compatibility_caution": "superplan_plus_unclear"},
    {"product_id": "kaldewei-ka-120-flat", "product_name": "KALDEWEI KA 120 flat", "product_url": SEEDS["conoflat"], "family": "ka_120", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "current_status": "current", "compatibility_caution": "superplan_plus_unclear"},
    {"product_id": "kaldewei-ka-120-vertical", "product_name": "KALDEWEI KA 120 vertical", "product_url": SEEDS["conoflat"], "family": "ka_120", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "current_status": "current", "compatibility_caution": "superplan_plus_unclear"},
    {"product_id": "kaldewei-ka-300-horizontal", "product_name": "KALDEWEI KA 300 horizontal", "product_url": SEEDS["calima"], "family": "ka_300", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "flow_rate_lps": 0.61, "height_adj_min_mm": 112, "height_adj_max_mm": 112},
    {"product_id": "kaldewei-ka-300-flat", "product_name": "KALDEWEI KA 300 flat", "product_url": SEEDS["calima"], "family": "ka_300", "candidate_type": "component", "system_role": "waste_fitting", "complete_system": "component", "flow_rate_lps": 0.57, "height_adj_min_mm": 92, "height_adj_max_mm": 92},
    {"product_id": "kaldewei-ka-4121", "product_name": "KALDEWEI KA 4121 NEXSYS drain set", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "system_role": "drain_set", "complete_system": "component", "promotion_reason": "drain_set_component", "current_status": "performance_data_unclear", "compatibility_caution": "requires_table_cell_validation"},
    {"product_id": "kaldewei-ka-4122", "product_name": "KALDEWEI KA 4122 NEXSYS drain set", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "system_role": "drain_set", "complete_system": "component", "promotion_reason": "drain_set_component", "current_status": "performance_data_unclear", "compatibility_caution": "requires_table_cell_validation"},
    {"product_id": "kaldewei-nexsys-design-cover-brushed", "product_name": "KALDEWEI NEXSYS design cover brushed", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "system_role": "design_cover", "complete_system": "component", "promotion_reason": "cover_only_component"},
    {"product_id": "kaldewei-nexsys-design-cover-polished", "product_name": "KALDEWEI NEXSYS design cover polished", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "system_role": "design_cover", "complete_system": "component", "promotion_reason": "cover_only_component"},
    {"product_id": "kaldewei-nexsys-design-cover-coated-white", "product_name": "KALDEWEI NEXSYS design cover coated white", "product_url": SEEDS["nexsys"], "family": "nexsys", "candidate_type": "component", "system_role": "design_cover", "complete_system": "component", "promotion_reason": "cover_only_component"},
    {"product_id": "kaldewei-ka-125-legacy", "product_name": "KALDEWEI KA 125", "product_url": SEEDS["waste"], "family": "ka_125", "candidate_type": "component", "system_role": "waste_fitting_legacy", "complete_system": "component", "current_status": "legacy_or_current_unclear"},
    {"product_id": "kaldewei-xetis-ka-200", "product_name": "KALDEWEI XETIS / KA 200", "product_url": SEEDS["xetis"], "family": "xetis", "candidate_type": "drain", "system_role": "complete_system", "complete_system": "yes", "current_status": "current_unclear", "promotion_reason": "current_status_unclear"},
]

BOM = {
    "kaldewei-flowline-zero": [("kaldewei-flowdrain-horizontal-regular", "required_trap_set"), ("kaldewei-flowdrain-horizontal-flat", "required_trap_set")],
    "kaldewei-flowpoint-zero": [("kaldewei-flowdrain-horizontal-regular", "required_trap_set"), ("kaldewei-flowdrain-horizontal-flat", "required_trap_set")],
    "kaldewei-nexsys": [("kaldewei-ka-4121", "compatible_drain_set"), ("kaldewei-ka-4122", "compatible_drain_set"), ("kaldewei-nexsys-design-cover-brushed", "compatible_cover"), ("kaldewei-nexsys-design-cover-polished", "compatible_cover"), ("kaldewei-nexsys-design-cover-coated-white", "compatible_cover")],
}


def discover_candidates(target_length_mm: int = 1200, tolerance_mm: int = 100) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows = []
    for r in CATALOG:
        row = dict(r)
        row["manufacturer"] = "kaldewei"
        row["product_family"] = str(row.get("family") or "unknown")
        base_url = str(row.get("product_url") or "")
        row["product_url"] = f"{base_url}#{row.get('product_id')}"
        row.setdefault("selected_length_mm", target_length_mm)
        rows.append(row)
    debug = [{"site": "kaldewei", "method": "seed_catalog", "candidates_found": len(rows), "seed_count": len(SEEDS)}]
    return rows, debug


def extract_parameters(url: str) -> Dict[str, Any]:
    u = (url or "").lower()
    frag = u.split("#")[-1] if "#" in u else ""
    if frag:
        row = next((r for r in CATALOG if str(r.get("product_id") or "").lower() == frag), None)
        if row:
            return {k: v for k, v in row.items() if k in {"flow_rate_lps", "outlet_dn", "height_adj_min_mm", "height_adj_max_mm", "water_seal_mm", "current_status", "compatibility_caution"}}
    for r in CATALOG:
        if str(r.get("product_url") or "").lower() == u:
            return {k: v for k, v in r.items() if k in {"flow_rate_lps", "outlet_dn", "height_adj_min_mm", "height_adj_max_mm", "water_seal_mm", "current_status", "compatibility_caution"}}
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
            "option_family": str(comp.get("family") or ""),
            "option_role": str(comp.get("system_role") or "component"),
            "parent_family": str(parent.get("family") or ""),
            "source_url": str(parent.get("product_url") or ""),
            "option_meta": f"{pid}:{opt_type}",
        })
    return out
