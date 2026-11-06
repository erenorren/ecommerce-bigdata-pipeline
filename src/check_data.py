import pandas as pd
import glob
import os

files = sorted(glob.glob("/opt/airflow/data/raw/*.xlsx"))
schemas = {}

for f in files:
    d = pd.read_excel(f)
    kolom = tuple(d.columns)
    if kolom not in schemas:
        schemas[kolom] = []
    schemas[kolom].append(os.path.basename(f))

print(f"Total File: {len(files)}")
for i, (kolom, daftar_file) in enumerate(schemas.items(), 1):
    print(f"\n--- Skema {i} ({len(daftar_file)} file) ---")
    print(f"Jumlah Kolom: {len(kolom)}")
    print(f"Daftar Kolom: {kolom}")
    print(f"File: {daftar_file}")