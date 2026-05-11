from __future__ import annotations
from typing import Any, Dict, Optional, Tuple, Union
import math
import re

from .config import WeightConfig

def _cfg_get(cfg, key, default=None):
    return cfg.get(key, default) if not isinstance(cfg, WeightConfig) else cfg.get(key, default)

def _to_float(x):
    try:
        if x is None or str(x).strip()=="": return None
        return float(x)
    except Exception:
        pass

    text = str(value).strip().lower()
    return text in {"", "nan", "none", "null", "unknown", "not_applicable", "n/a"}


def _to_float(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None

def _truthy(x):
    s=str(x or '').strip().lower()
    return s in {'yes','true','1','confirmed','preassembled','included_preassembled'}

def _contains(row,*tokens):
    txt=' '.join(str(row.get(k) or '') for k in row.keys()).lower()
    return any(t.lower() in txt for t in tokens)

def compute_parameter_score(params: Dict[str, Any], cfg: Union[Dict[str, Any], WeightConfig]) -> Tuple[float, Dict[str, float]]:
    d: Dict[str, float|str] = {}
    flow=_to_float(params.get('flow_rate_lps'))
    d['flow_rate_score']=1.0 if flow and flow>=0.8 else (max(0.0,flow/0.8) if flow and flow>0 else 0.0)
    d['flow_rate_pass_0_8_lps']='yes' if flow and flow>=0.8 else ('no' if flow and flow>0 else 'unknown')

    mat=' '.join(str(params.get(k) or '') for k in ('material_v4a','material_detail','material_class')).lower()
    if any(t in mat for t in ('v4a','1.4404','1.4571','316l','316')): d['material_v4a_score']=1.0
    elif any(t in mat for t in ('v2a','1.4301','304')): d['material_v4a_score']=0.5
    elif 'stainless' in mat: d['material_v4a_score']=0.4
    elif any(t in mat for t in ('polypropylene','abs','plastic')): d['material_v4a_score']=0.2
    else: d['material_v4a_score']=0.0

    certtxt=' '.join(str(params.get(k) or '') for k in ('din_en_1253','certification_din_en_1253','certifications','certificate_text','din_en_1253_cert')).lower()
    d['din_en_1253_score']=1.0 if ('din en 1253' in certtxt or _truthy(params.get('din_en_1253')) or _truthy(params.get('din_en_1253_cert'))) else 0.0
    cert185=' '.join(str(params.get(k) or '') for k in ('din_en_18534','certification_din_en_18534','certifications','waterproofing_standard','certificate_text','din_18534_compliance')).lower()
    d['din_en_18534_score']=1.0 if ('din en 18534' in cert185 or _truthy(params.get('din_en_18534')) or _truthy(params.get('din_18534_compliance'))) else 0.0

    hmin,hmax=_to_float(params.get('height_adj_min_mm')),_to_float(params.get('height_adj_max_mm'))
    rng=(hmax-hmin) if (hmin is not None and hmax is not None and hmax>hmin) else 0.0
    d['height_adjustability_range_mm']=rng
    d['height_adjustability_score']=min(rng/100.0,1.0) if rng>0 else 0.0

    out=' '.join(str(params.get(k) or '') for k in ('outlet_orientation','outlet_options')).lower()
    v=bool(params.get('vertical_outlet_available')) or 'vertical' in out
    s=bool(params.get('side_outlet_available')) or 'side' in out or 'horizontal' in out
    d['outlet_flexibility_score']=1.0 if (v and s) else (0.5 if (v or s) else 0.0)

    fleece=' '.join(str(params.get(k) or '') for k in ('sealing_fleece_preassembled','sealing_fleece','waterproofing_fleece_preassembled')).lower()
    d['sealing_fleece_score']=1.0 if ('preassembled' in fleece or _truthy(params.get('sealing_fleece_preassembled'))) else (0.5 if 'included' in fleece else 0.0)

    cc=_to_float(params.get('colours_count') or params.get('color_count') or params.get('finish_count'))
    d['colour_count_score']=min((cc or 0)/5.0,1.0) if cc and cc>0 else 0.0

    d['sales_price_score']=0.0
    d['scoring_price_available']='unknown'
    d['scoring_notes']='benchmark_scoring'

    weights=dict(_cfg_get(cfg,'final_weights_pct',{}) or {})
    keys=['flow_rate_score','material_v4a_score','din_en_1253_score','din_en_18534_score','height_adjustability_score','sales_price_score','outlet_flexibility_score','sealing_fleece_score','colour_count_score']
    wsum=sum(float(weights.get(k,0)) for k in keys) or 1.0
    total=sum(float(d.get(k,0))*float(weights.get(k,0))/wsum for k in keys)
    return float(total), d

def compute_equivalence_score(row, cfg):
    return 0.0

def compute_system_score(candidate_type: str, has_bom_options: bool) -> float:
    return 1.0 if str(candidate_type).lower()=='drain' else 0.5

def compute_final_score(param_score: float, system_score: float, equiv_score: float, cfg) -> float:
    w_param=float(_cfg_get(cfg,'w_param',1.0) or 1.0)
    w_system=float(_cfg_get(cfg,'w_system',0.0) or 0.0)
    w_equiv=float(_cfg_get(cfg,'w_equiv',0.0) or 0.0)
    if any(w>1 for w in (w_param,w_system,w_equiv)):
        w_param,w_system,w_equiv=w_param/100.0,w_system/100.0,w_equiv/100.0
    s=(w_param+w_system+w_equiv) or 1.0
    out=(param_score*w_param + system_score*w_system + equiv_score*w_equiv)/s
    return max(0.0,min(1.0,float(out)))
