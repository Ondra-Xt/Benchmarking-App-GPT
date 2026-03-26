python - << "PY"
import pandas as pd

path = r"data\runs\export_20260302_122802\outputs\benchmark_output.xlsx"
pid = input("Zadej product_id z předchozího výpisu: ").strip()

ev = pd.read_excel(path, sheet_name="Evidence")
ev_pid = ev[ev["product_id"].astype(str) == pid]

print("Evidence rows for", pid, ":", len(ev_pid))
cols = [c for c in ["label","value","source","manufacturer","product_id"] if c in ev_pid.columns]
print(ev_pid[cols].head(40).to_string(index=False))
PY