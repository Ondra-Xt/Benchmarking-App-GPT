import pandas as pd

CSV = "products.csv"  # spouštěj ze složky ...\outputs

cols = [
    "flow_rate_lps",
    "din_en_1253_cert",
    "din_18534_compliance",
    "material_v4a",
    "material_detail",
    "height_adj_min_mm",
    "height_adj_max_mm",
    "outlet_dn",
]

df = pd.read_csv(CSV)

print("rows", len(df))
print("\nmanufacturer counts:")
print(df["manufacturer"].value_counts(dropna=False))

def miss_stats(d: pd.DataFrame) -> pd.Series:
    miss = {}
    for c in cols:
        if c in d.columns:
            miss[c] = round(float(d[c].isna().mean() * 100), 2)
        else:
            miss[c] = None
    return pd.Series(miss).sort_values(ascending=False)

print("\n=== MISSING % (ALL) ===")
print(miss_stats(df))

# Missing podle výrobce
print("\n=== MISSING % BY MANUFACTURER ===")
for m, g in df.groupby(df["manufacturer"].fillna("")):
    print(f"\n--- {m!r} rows={len(g)} ---")
    print(miss_stats(g))

# Hansgrohe fertigset filtr (pokud existuje product_url)
if "product_url" in df.columns:
    h = df[df["manufacturer"].astype(str).str.lower().eq("hansgrohe")].copy()
    h["u"] = h["product_url"].astype(str).str.lower()
    print("\n=== HANSGROHE ONLY ===")
    print("hansgrohe rows", len(h))
    fertig = h[h["u"].str.contains("fertigset", na=False)]
    print("fertigset rows", len(fertig))
    if len(fertig) > 0:
        print("\n=== HANSGROHE fertigset missing % ===")
        print(miss_stats(fertig))
else:
    print("\nNOTE: column 'product_url' not found in products.csv (cannot do fertigset filter).")
