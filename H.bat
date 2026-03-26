python - << "PY"
import pandas as pd

path = r"data\runs\export_20260302_122802\outputs\benchmark_output.xlsx"
df = pd.read_excel(path, sheet_name="Products")

miss = df[df["flow_rate_lps"].isna()]
print("Missing rows:", len(miss), "of", len(df))
print(miss[["manufacturer","product_id","candidate_type","product_name","product_url"]].to_string(index=False))
PY