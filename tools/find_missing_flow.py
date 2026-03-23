import pandas as pd
from pathlib import Path

path = Path(r"data\runs\export_20260302_122802\outputs\benchmark_output.xlsx")
df = pd.read_excel(path, sheet_name="Products")

miss = df[df["flow_rate_lps"].isna()]
print("Missing rows:", len(miss), "of", len(df))

cols = ["manufacturer","product_id","candidate_type","product_name","product_url"]
cols = [c for c in cols if c in df.columns]
if len(miss):
    print(miss[cols].to_string(index=False))
else:
    print("OK")