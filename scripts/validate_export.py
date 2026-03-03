#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


REQUIRED_SHEETS = ["Products", "Components", "Comparison", "Evidence", "BOM_Options"]
DEFAULT_RUNS_DIR = Path("data/runs")


def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    print("FAIL")
    raise SystemExit(1)


def warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def ok(msg: str) -> None:
    print(f"[OK] {msg}")


def find_latest_export(runs_dir: Path) -> Path:
    if not runs_dir.exists():
        fail(f"Adresář pro běhy neexistuje: {runs_dir}")

    candidates = [
        p
        for p in runs_dir.glob("*/outputs/*.xlsx")
        if p.is_file() and not p.name.startswith("~$")
    ]

    if not candidates:
        fail(f"Nebyl nalezen žádný export .xlsx v {runs_dir}/*/outputs")

    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    ok(f"Použit nejnovější export: {latest}")
    return latest


def load_sheet(xls: pd.ExcelFile, name: str) -> pd.DataFrame:
    if name not in xls.sheet_names:
        fail(f"Chybí sheet '{name}'.")
    return pd.read_excel(xls, sheet_name=name)


def validate_export(xlsx_path: Path) -> None:
    if not xlsx_path.exists():
        fail(f"Soubor neexistuje: {xlsx_path}")

    xls = pd.ExcelFile(xlsx_path)

    for sheet in REQUIRED_SHEETS:
        if sheet not in xls.sheet_names:
            fail(f"Chybí sheet: {sheet}")
    ok("Všechny povinné sheet-y existují.")

    products = load_sheet(xls, "Products")
    components = load_sheet(xls, "Components")
    comparison = load_sheet(xls, "Comparison")
    evidence = load_sheet(xls, "Evidence")
    bom = load_sheet(xls, "BOM_Options")

    if len(comparison) != len(products) + len(components):
        fail(
            f"Comparison rows ({len(comparison)}) != Products+Components ({len(products) + len(components)})."
        )
    ok("Comparison row count sedí (Products + Components).")

    for col in ["manufacturer", "product_id", "candidate_type", "product_url"]:
        if col not in products.columns:
            fail(f"Products nemá sloupec '{col}'.")
        if col not in components.columns:
            fail(f"Components nemá sloupec '{col}'.")
    ok("Základní sloupce v Products/Components existují.")

    if products["product_id"].duplicated().any():
        dups = products.loc[products["product_id"].duplicated(keep=False), "product_id"].tolist()
        fail(f"Duplicitní product_id v Products: {sorted(set(dups))[:20]} ...")
    if components["product_id"].duplicated().any():
        dups = components.loc[components["product_id"].duplicated(keep=False), "product_id"].tolist()
        fail(f"Duplicitní product_id v Components: {sorted(set(dups))[:20]} ...")
    ok("product_id je unikátní v Products i Components.")

    inter = set(products["product_id"]).intersection(set(components["product_id"]))
    if inter:
        fail(f"product_id je zároveň v Products i Components: {sorted(inter)[:20]} ...")
    ok("Žádný product_id se neopakuje mezi Products a Components.")

    for col in ["flow_rate_lps", "outlet_dn"]:
        if col not in products.columns:
            warn(f"Products nemá '{col}' (nelze kontrolovat).")
            continue
        missing = products[col].isna().mean()
        if missing > 0:
            if col == "flow_rate_lps":
                missing_rows = products[products[col].isna()].copy()
                show_cols = [c for c in ["product_id", "product_url"] if c in missing_rows.columns]
                if show_cols:
                    preview = missing_rows[show_cols].head(20).to_dict(orient="records")
                    fail(
                        f"Products: '{col}' missing {missing * 100:.1f} % (čekáme 0 %). "
                        f"Chybějící řádky (max 20): {preview}"
                    )
            fail(f"Products: '{col}' missing {missing * 100:.1f} % (čekáme 0 %).")
    ok("flow_rate_lps a outlet_dn jsou kompletní (0 % missing).")

    hg_prod = products[products["manufacturer"].astype(str).str.lower() == "hansgrohe"].copy()
    hg_comp = components[components["manufacturer"].astype(str).str.lower() == "hansgrohe"].copy()

    hg_finish = hg_prod[hg_prod["candidate_type"].astype(str).str.lower() == "finish_set"]
    expected_bom = 2 * len(hg_finish)

    if len(bom) != expected_bom:
        fail(f"BOM_Options rows ({len(bom)}) != 2 * hansgrohe finish_set ({expected_bom}).")
    ok("BOM_Options = 2 × počet hansgrohe finish_set.")

    if len(bom) > 0:
        if "product_id" not in bom.columns or "bom_code" not in bom.columns:
            fail("BOM_Options nemá očekávané sloupce (product_id, bom_code).")

        bom_pids = set(bom["product_id"].astype(str))
        finish_pids = set(hg_finish["product_id"].astype(str))
        extra = bom_pids - finish_pids
        if extra:
            fail(
                "BOM_Options obsahuje product_id, které nejsou finish_set v Products: "
                f"{sorted(extra)[:20]} ..."
            )
        ok("BOM_Options odkazuje pouze na hansgrohe finish_set produkty.")

    ubox_ids = {"hansgrohe-01000180", "hansgrohe-01001180"}
    present_comp = set(hg_comp["product_id"].astype(str))
    missing_ubox = ubox_ids - present_comp
    if missing_ubox:
        warn(f"Chybí uBox base_set v Components: {sorted(missing_ubox)}")

    ubox_in_products = ubox_ids.intersection(set(hg_prod["product_id"].astype(str)))
    if ubox_in_products:
        fail(f"uBox je špatně v Products: {sorted(ubox_in_products)}")
    ok("uBox base-set není v Products (správně).")

    for col in ["manufacturer", "product_id", "label", "source"]:
        if col not in evidence.columns:
            warn(f"Evidence nemá sloupec '{col}' (audit trail bude slabší).")
    ok("Evidence sheet existuje a má očekávanou strukturu (základní).")

    if "height_adj_min_mm" in products.columns:
        miss_hg_height = hg_prod["height_adj_min_mm"].isna().mean() * 100
        if miss_hg_height > 50:
            warn(f"Hansgrohe height missing {miss_hg_height:.1f}% (další krok: parsing výšek z PDF).")

    if "height_adj_max_mm" in hg_prod.columns:
        suspicious_low = hg_prod[hg_prod["height_adj_max_mm"].notna() & (hg_prod["height_adj_max_mm"] <= 30)]
        if not suspicious_low.empty:
            cols = [
                c
                for c in ["product_id", "product_url", "height_adj_min_mm", "height_adj_max_mm"]
                if c in suspicious_low.columns
            ]
            warn(
                "Hansgrohe suspicious height <= 30 mm: "
                f"{suspicious_low[cols].head(20).to_dict(orient='records')}"
            )

    if "height_adj_min_mm" in hg_prod.columns and "height_adj_max_mm" in hg_prod.columns:
        suspicious_mixed = hg_prod[
            hg_prod["height_adj_min_mm"].notna()
            & hg_prod["height_adj_max_mm"].notna()
            & (hg_prod["height_adj_min_mm"] <= 20)
            & (hg_prod["height_adj_max_mm"] >= 50)
        ]
        if not suspicious_mixed.empty:
            cols = [
                c
                for c in ["product_id", "product_url", "height_adj_min_mm", "height_adj_max_mm"]
                if c in suspicious_mixed.columns
            ]
            warn(
                "Hansgrohe suspicious mixed height (tile depth + install height): "
                f"{suspicious_mixed[cols].head(20).to_dict(orient='records')}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validace benchmark exportu XLSX.")
    parser.add_argument("--xlsx", type=Path, help="Cesta k exportovanému XLSX souboru.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    xlsx_path = args.xlsx if args.xlsx else find_latest_export(DEFAULT_RUNS_DIR)

    validate_export(xlsx_path)
    print("PASS")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] Neočekávaná chyba: {exc}")
        print("FAIL")
        raise SystemExit(1)
