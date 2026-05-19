from __future__ import annotations
import streamlit as st
from pathlib import Path
import platform
import subprocess
import sys
import pandas as pd
import subprocess
import sys
import platform
import os


from src.config import load_config, save_config, default_config, EQUIVALENCE_KEYS, FINAL_KEYS, validate_sum_100
from src.run_manager import utc_run_id, create_run_dirs
from src.pipeline import run_discovery, run_update
from src.excel_export import export_excel
from src.connectors import CONNECTORS
from src.connectors import aco as aco_connector

APP_TITLE = "Drain Systems Benchmark – MVP"
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CONFIG_PATH = DATA_DIR / "config" / "weights.json"
TEMPLATE_PATH = DATA_DIR / "templates" / "benchmark_template.xlsx"

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)



def _git_cmd(args: list[str]) -> str:
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:
        return f"unknown ({type(exc).__name__}: {exc})"


def _fixture_exists(relative_path: str) -> bool:
    return (BASE_DIR / relative_path).exists()


def _http_probe(url: str, timeout: int = 10) -> dict[str, str]:
    try:
        import requests

        r = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 DrainBenchmarkDebug/1.0",
                "Accept": "text/html,application/pdf,*/*",
            },
            allow_redirects=True,
        )
        return {
            "status": str(r.status_code),
            "final_url": r.url,
            "content_type": r.headers.get("content-type", ""),
            "bytes": str(len(r.content or b"")),
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "final_url": "",
            "content_type": "",
            "bytes": "0",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _clear_app_session_state() -> None:
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

# Load config
cfg = load_config(CONFIG_PATH)

# Sidebar: settings
st.sidebar.header("Settings")

with st.sidebar.expander("Runtime debug", expanded=False):
    st.write("Python:", sys.version.split()[0])
    st.write("Python full:", sys.version)
    st.write("Platform:", platform.platform())
    st.write("Working dir:", os.getcwd())
    st.write("Git branch:", _git_cmd(["git", "branch", "--show-current"]))
    st.write("Git commit:", _git_cmd(["git", "rev-parse", "--short", "HEAD"]))
    st.write("Fixtures / S+ family:", _fixture_exists("tests/fixtures/aco_splus/splus_family.html"))
    st.write("Fixtures / S+ profile:", _fixture_exists("tests/fixtures/aco_splus/splus_profile.html"))
    st.write("Fixtures / S+ drain body:", _fixture_exists("tests/fixtures/aco_splus/splus_drain_body.html"))
    st.write("Fixtures / S+ brochure:", _fixture_exists("tests/fixtures/aco_splus/splus_brochure.pdf"))

    if st.button("Reset in-app session state", help="Vyčistí registry/products/comparison/evidence uložené v aktuální Streamlit session."):
        _clear_app_session_state()

    if st.button("Probe ACO S+ sources", help="Ověří HTTP status z prostředí, kde aplikace běží."):
        probe_urls = {
            "splus_family": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/",
            "splus_profile": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/",
            "splus_drain_body": "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/",
            "splus_brochure_pdf": "https://www.aco-haustechnik.de/fileadmin/aco_haustechnik/documents/Prospekte-PDF/Prospekt-ACO_Sanit%C3%A4r_Duschrinne_ShowerDrain_S-Plus.pdf",
        }
        rows = []
        for name, url in probe_urls.items():
            result = _http_probe(url)
            rows.append({"source": name, "url": url, **result})
        st.dataframe(pd.DataFrame(rows), width="stretch", height=220)

target_length = st.sidebar.number_input("Target length (mm)", min_value=300, max_value=3000, value=1200, step=10)
tolerance = st.sidebar.number_input("Tolerance (±mm)", min_value=0, max_value=500, value=100, step=10)

show_excluded = st.sidebar.checkbox("Show excluded products", value=False)
connector_options = sorted(CONNECTORS.keys())
selected_connectors = st.sidebar.multiselect(
    "Connectors to run",
    options=connector_options,
    default=connector_options,
)

st.sidebar.subheader("Penalties")
cfg.unknown_penalty_score = st.sidebar.number_input("Unknown score (0–1)", min_value=0.0, max_value=1.0, value=float(cfg.unknown_penalty_score), step=0.05)

st.sidebar.divider()
st.sidebar.subheader("Benchmark scoring weights (sum 100%)")

fin_weights = dict(cfg.final_weights_pct)
final_auto_key = st.sidebar.selectbox("Auto-balance key (benchmark scoring)", options=FINAL_KEYS, index=0)
fin_sum_manual = 0
for k in FINAL_KEYS:
    if k == final_auto_key:
        continue
    fin_weights[k] = st.sidebar.number_input(f"{k} (%)", min_value=0, max_value=100, value=int(fin_weights.get(k, 0)), step=1, key=f"fin_{k}")
    fin_sum_manual += int(fin_weights[k])
fin_weights[final_auto_key] = max(0, 100 - fin_sum_manual)
st.sidebar.caption(f"{final_auto_key} auto-balanced to {fin_weights[final_auto_key]} % (sum = 100%)")

with st.sidebar.expander("Advanced / legacy equivalence scoring", expanded=False):
    eq_auto_key = st.selectbox("Auto-balance key (legacy equivalence)", options=EQUIVALENCE_KEYS, index=EQUIVALENCE_KEYS.index("length_mode_match"))
    eq_weights = dict(getattr(cfg, "equivalence_weights_pct", {}))
    eq_sum_manual = 0
    for k in EQUIVALENCE_KEYS:
        if k == eq_auto_key:
            continue
        eq_weights[k] = st.number_input(f"{k} (%)", min_value=0, max_value=100, value=int(eq_weights.get(k, 0)), step=1, key=f"eq_{k}")
        eq_sum_manual += int(eq_weights[k])
    eq_weights[eq_auto_key] = max(0, 100 - eq_sum_manual)
    st.caption(f"{eq_auto_key} auto-balanced to {eq_weights[eq_auto_key]} % (sum = 100%)")
st.sidebar.divider()

st.sidebar.subheader("Runtime debug")

def _git_cmd(args: list[str]) -> str:
    try:
        out = subprocess.check_output(args, cwd=BASE_DIR, stderr=subprocess.DEVNULL, text=True).strip()
        return out or "unknown"
    except Exception:
        return "unknown"

branch = _git_cmd(["git", "rev-parse", "--abbrev-ref", "HEAD"])
commit_short = _git_cmd(["git", "rev-parse", "--short", "HEAD"])

fixtures_dir = BASE_DIR / "tests" / "fixtures" / "aco_splus"
fixture_profile = fixtures_dir / "splus_profile.html"
fixture_drain = fixtures_dir / "splus_drain_body.html"
fixture_pdf = fixtures_dir / "splus_brochure.pdf"

st.sidebar.caption(f"Python: `{sys.version.split()[0]}`")
st.sidebar.caption(f"Platform: `{platform.platform()}`")
st.sidebar.caption(f"Git branch: `{branch}`")
st.sidebar.caption(f"Git commit: `{commit_short}`")
st.sidebar.caption(f"`{fixture_profile.as_posix()}` exists: **{fixture_profile.exists()}**")
st.sidebar.caption(f"`{fixture_drain.as_posix()}` exists: **{fixture_drain.exists()}**")
st.sidebar.caption(f"`{fixture_pdf.as_posix()}` exists: **{fixture_pdf.exists()}**")

with st.sidebar.expander("ACO source diagnostics (optional)", expanded=False):
    splus_urls = [
        ("S+ family", "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/"),
        ("S+ profile", "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/aco-showerdrain-splus-duschrinnenprofil/"),
        ("S+ drain-body", "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-splus/ablaufkoerper-zu-aco-duschrinnenprofil-showerdrain-splus/"),
    ]
    for label, url in splus_urls:
        try:
            status, final_url, _html, err = aco_connector._safe_get_text(url, timeout=20)
            st.caption(f"{label}: status={status} err={err or '-'} final={final_url}")
        except Exception as ex:
            st.caption(f"{label}: status=error err={ex}")

if st.sidebar.button("Save settings"):
    cfg.equivalence_weights_pct = {k: int(eq_weights.get(k, 0)) for k in EQUIVALENCE_KEYS}
    cfg.final_weights_pct = {k: int(fin_weights.get(k, 0)) for k in FINAL_KEYS}
    save_config(CONFIG_PATH, cfg)
    st.sidebar.success("Settings saved.")

# Main actions
col1, col2, col3 = st.columns([1,1,1])
with col1:
    run_discovery_btn = st.button("Run discovery", width='stretch')
with col2:
    run_update_btn = st.button("Run update", width='stretch')
with col3:
    export_btn = st.button("Export Excel", width='stretch')

# Session state
if "registry" not in st.session_state:
    st.session_state["registry"] = pd.DataFrame()
if "discovery_debug" not in st.session_state:
    st.session_state["discovery_debug"] = pd.DataFrame()
if "products" not in st.session_state:
    st.session_state["products"] = pd.DataFrame()
if "comparison" not in st.session_state:
    st.session_state["comparison"] = pd.DataFrame()
if "excluded" not in st.session_state:
    st.session_state["excluded"] = pd.DataFrame()
if "evidence" not in st.session_state:
    st.session_state["evidence"] = pd.DataFrame()
if "bom_options" not in st.session_state:
    st.session_state["bom_options"] = pd.DataFrame()
if "last_run_dir" not in st.session_state:
    st.session_state["last_run_dir"] = ""


def _reset_update_state() -> None:
    st.session_state["products"] = pd.DataFrame()
    st.session_state["comparison"] = pd.DataFrame()
    st.session_state["excluded"] = pd.DataFrame()
    st.session_state["evidence"] = pd.DataFrame()
    st.session_state["bom_options"] = pd.DataFrame()

# Run discovery
if run_discovery_btn:
    run_id = utc_run_id("discovery")
    rp = create_run_dirs(DATA_DIR, run_id)
    # snapshot weights
    save_config(rp.run_dir / "weights.json", cfg)

    reg, dbg = run_discovery(
        target_length_mm=int(target_length),
        tolerance_mm=int(tolerance),
        selected_connectors=selected_connectors,
    )
    reg.to_csv(rp.outputs_dir / "registry.csv", index=False)
    dbg.to_csv(rp.outputs_dir / "discovery_debug.csv", index=False)
    st.session_state["registry"] = reg
    st.session_state["discovery_debug"] = dbg
    _reset_update_state()
    st.session_state["last_run_dir"] = str(rp.run_dir)

    st.success(f"Discovery completed. Candidates: {len(reg)}. Run: {run_id}")
    if len(reg) == 0 and not dbg.empty:
        st.warning("Discovery found no candidates. HTTP diagnostics are shown below.")
        st.dataframe(dbg, width='stretch', height=240)

# Run update
if run_update_btn:
    run_id = utc_run_id("update")
    rp = create_run_dirs(DATA_DIR, run_id)
    save_config(rp.run_dir / "weights.json", cfg)

    reg = st.session_state["registry"]
    if reg.empty:
        st.warning("Run discovery first, or load a registry.")
    else:
        products, comparison, excluded, evidence, bom_options = run_update(
            reg,
            cfg,
            target_length_mm=int(target_length),
            tolerance_mm=int(tolerance),
            selected_connectors=selected_connectors,
        )
        st.session_state["products"] = products
        st.session_state["comparison"] = comparison
        st.session_state["excluded"] = excluded
        st.session_state["evidence"] = evidence
        st.session_state["bom_options"] = bom_options

        # persist snapshots
        reg.to_csv(rp.outputs_dir / "registry.csv", index=False)
        products.to_csv(rp.outputs_dir / "products.csv", index=False)
        comparison.to_csv(rp.outputs_dir / "comparison.csv", index=False)
        excluded.to_csv(rp.outputs_dir / "excluded.csv", index=False)
        evidence.to_csv(rp.outputs_dir / "evidence.csv", index=False)
        bom_options.to_csv(rp.outputs_dir / "bom_options.csv", index=False)

        st.session_state["last_run_dir"] = str(rp.run_dir)
        st.success(f"Update completed. Eligible: {len(comparison)}. Excluded: {len(excluded)}. Run: {run_id}")

# Results
st.header("Results")

summary_cols = st.columns(4)
summary_cols[0].metric("Registry", len(st.session_state["registry"]))
summary_cols[1].metric("Eligible (Comparison)", len(st.session_state["comparison"]))
summary_cols[2].metric("Excluded", len(st.session_state["excluded"]))
summary_cols[3].metric("Evidence rows", len(st.session_state["evidence"]))

st.subheader("Discovery debug")
if not st.session_state["discovery_debug"].empty:
    st.dataframe(st.session_state["discovery_debug"].tail(200), width='stretch', height=220)

st.subheader("Comparison (eligible)")
if st.session_state["comparison"].empty:
    st.info("No results yet. Run update first.")
else:
    st.dataframe(st.session_state["comparison"], width='stretch', height=360)

if show_excluded:
    st.subheader("Excluded")
    st.dataframe(st.session_state["excluded"], width='stretch', height=240)

st.subheader("Evidence (audit)")
if not st.session_state["evidence"].empty:
    st.dataframe(st.session_state["evidence"].tail(200), width='stretch', height=240)

# Export Excel
if export_btn:
    if st.session_state["comparison"].empty and st.session_state["registry"].empty:
        st.warning("Nothing to export. Run discovery/update.")
    else:
        run_id = utc_run_id("export")
        rp = create_run_dirs(DATA_DIR, run_id)
        save_config(rp.run_dir / "weights.json", cfg)

        out_path = rp.outputs_dir / "benchmark_output.xlsx"
        export_excel(
            TEMPLATE_PATH, out_path, cfg,
            registry_df=st.session_state["registry"],
            products_df=st.session_state["products"],
            comparison_df=st.session_state["comparison"],
            excluded_df=st.session_state["excluded"],
            evidence_df=st.session_state["evidence"],
            bom_options_df=st.session_state["bom_options"],
            components_df=None,
        )
        st.session_state["registry"].to_csv(rp.outputs_dir / "registry.csv", index=False)
        st.session_state["products"].to_csv(rp.outputs_dir / "products.csv", index=False)
        st.session_state["comparison"].to_csv(rp.outputs_dir / "comparison.csv", index=False)
        st.session_state["excluded"].to_csv(rp.outputs_dir / "excluded.csv", index=False)
        st.session_state["evidence"].to_csv(rp.outputs_dir / "evidence.csv", index=False)
        st.session_state["bom_options"].to_csv(rp.outputs_dir / "bom_options.csv", index=False)
        st.session_state["last_run_dir"] = str(rp.run_dir)
        st.success("Excel export completed.")
        with open(out_path, "rb") as f:
            st.download_button("Download Excel", data=f, file_name="benchmark_output.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.caption(f"Data folder: {DATA_DIR} | Last run: {st.session_state['last_run_dir']}")
