import os
import glob
import argparse
import pandas as pd

def convert_excel_to_parquet(month_str=None):
    raw_dir = "/opt/airflow/data/raw"
    staging_dir = "/opt/airflow/data/staging"
    os.makedirs(staging_dir, exist_ok=True)

    search_pattern = os.path.join(raw_dir, f"*{month_str}*.xlsx") if month_str else os.path.join(raw_dir, "*.xlsx")
    files = glob.glob(search_pattern)

    if not files:
        search_pattern = os.path.join(raw_dir, f"*{month_str}*.csv") if month_str else os.path.join(raw_dir, "*.csv")
        files = glob.glob(search_pattern)

    if not files:
        print(f"[WARNING] Tidak ditemukan file data mentah.")
        return

    print(f"[INFO] Total file ditemukan di {raw_dir}: {len(files)} file")

    for file_path in files:
        file_name = os.path.basename(file_path)
        base_name = os.path.splitext(file_name)[0]
        output_path = os.path.join(staging_dir, f"{base_name}.parquet")

        print(f"[INFO] Memproses: {file_name} -> {output_path}")

        # 1. Baca data sesuai format
        if file_path.endswith(".xlsx"):
            df = pd.read_excel(file_path, engine="openpyxl")
        else:
            df = pd.read_csv(file_path)

        # 2. PERBAIKAN AUDIT CLAUDE: Tambahkan kolom source_file untuk pelacakan
        df["source_file"] = file_name

        # 3. KUNCI PERBAIKAN: Hapus df = df.astype(str) agar tipe data asli (angka) tidak rusak
        if "Waktu Pesanan Dibuat" in df.columns:
            df["Waktu Pesanan Dibuat"] = df["Waktu Pesanan Dibuat"].astype(str)

        # 4. Simpan ke Parquet
        df.to_parquet(output_path, index=False, engine="pyarrow")
        print(f"[SUCCESS] Berhasil mengonversi {file_name}")

    print("\n[FINISH] Seluruh file berhasil dikonversi ke Parquet!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", type=str, default=None)
    args = parser.parse_args()
    convert_excel_to_parquet(args.month)