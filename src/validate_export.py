import argparse
import sys
import re
import pandas as pd


REQUIRED_SHEETS = ["Products", "Components", "Comparison", "Evidence", "BOM_Options"]


def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(1)


def warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def ok(msg: str) -> None:
    print(f"[OK] {msg}")


def load_sheet(xls: pd.ExcelFile, name: str) -> pd.DataFrame:
    if name not in xls.sheet_names:
        fail(f"Chybí sheet '{name}'.")
    return pd.read_excel(xls, sheet_name=name)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx_path", help="Cesta k exportovanému Excelu (benchmark_output XX.xlsx)")
    args = ap.parse_args()

    xls = pd.ExcelFile(args.xlsx_path)

    for s in REQUIRED_SHEETS:
        if s not in xls.sheet_names:
            fail(f"Chybí sheet: {s}")
    ok("Všechny povinné sheet-y existují.")

    products = load_sheet(xls, "Products")
    components = load_sheet(xls, "Components")
    comparison = load_sheet(xls, "Comparison")
    evidence = load_sheet(xls, "Evidence")
    bom = load_sheet(xls, "BOM_Options")

    # --- Basic shape checks
    if len(comparison) != len(products) + len(components):
        fail(f"Comparison rows ({len(comparison)}) != Products+Components ({len(products)+len(components)}).")
    ok("Comparison row count sedí (Products + Components).")

    # --- Required columns
    for col in ["manufacturer", "product_id", "candidate_type", "product_url"]:
        if col not in products.columns:
            fail(f"Products nemá sloupec '{col}'.")
        if col not in components.columns:
            fail(f"Components nemá sloupec '{col}'.")
    ok("Základní sloupce v Products/Components existují.")

    # --- product_id uniqueness
    if products["product_id"].duplicated().any():
        d = products.loc[products["product_id"].duplicated(keep=False), "product_id"].tolist()
        fail(f"Duplicitní product_id v Products: {sorted(set(d))[:20]} ...")
    if components["product_id"].duplicated().any():
        d = components.loc[components["product_id"].duplicated(keep=False), "product_id"].tolist()
        fail(f"Duplicitní product_id v Components: {sorted(set(d))[:20]} ...")
    ok("product_id je unikátní v Products i Components.")

    inter = set(products["product_id"]).intersection(set(components["product_id"]))
    if inter:
        fail(f"product_id je zároveň v Products i Components: {sorted(inter)[:20]} ...")
    ok("Žádný product_id se neopakuje mezi Products a Components.")

    # --- Core KPI fields: must not be missing
    for col in ["flow_rate_lps", "outlet_dn"]:
        if col not in products.columns:
            warn(f"Products nemá '{col}' (nelze kontrolovat).")
            continue
        miss = products[col].isna().mean()
        if miss > 0:
            fail(f"Products: '{col}' missing {miss*100:.1f} % (čekáme 0 %).")
    ok("flow_rate_lps a outlet_dn jsou kompletní (0 % missing).")

    # --- Hansgrohe finish-set / BOM invariants
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

        # BOM should reference only hansgrohe finish_set product_ids
        bom_pids = set(bom["product_id"].astype(str))
        finish_pids = set(hg_finish["product_id"].astype(str))
        extra = bom_pids - finish_pids
        if extra:
            fail(f"BOM_Options obsahuje product_id, které nejsou finish_set v Products: {sorted(extra)[:20]} ...")
        ok("BOM_Options odkazuje pouze na hansgrohe finish_set produkty.")

    # uBox must be in Components as base_set and not in Products
    ubox_ids = {"hansgrohe-01000180", "hansgrohe-01001180"}
    present_comp = set(hg_comp["product_id"].astype(str))
    missing_ubox = ubox_ids - present_comp
    if missing_ubox:
        warn(f"Chybí uBox base_set v Components: {sorted(missing_ubox)}")

    ubox_in_products = ubox_ids.intersection(set(hg_prod["product_id"].astype(str)))
    if ubox_in_products:
        fail(f"uBox je špatně v Products: {sorted(ubox_in_products)}")
    ok("uBox base-set není v Products (správně).")

    # --- Evidence sanity
    for col in ["manufacturer", "product_id", "label", "source"]:
        if col not in evidence.columns:
            warn(f"Evidence nemá sloupec '{col}' (audit trail bude slabší).")
    ok("Evidence sheet existuje a má očekávanou strukturu (základní).")

    # --- Optional: height coverage warning
    if "height_adj_min_mm" in products.columns:
        miss_hg_height = hg_prod["height_adj_min_mm"].isna().mean() * 100
        if miss_hg_height > 50:
            warn(f"Hansgrohe height missing {miss_hg_height:.1f}% (další krok: parsing výšek z PDF).")

    ok("Validace dokončena.")
    print("[PASS] Export je konzistentní.")


if __name__ == "__main__":
    main()