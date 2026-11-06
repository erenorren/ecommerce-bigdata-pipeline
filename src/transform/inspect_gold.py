import pandas as pd

def main():
    path_gold = '/opt/airflow/data/gold/fact_orders.parquet'
    df = pd.read_parquet(path_gold)

    print("\n=== 1. DAFTAR KOLOM TABEL GOLD ===")
    print(df.columns.tolist())

    print("\n=== 2. DISTRIBUSI TARGET IS_BATAL ===")
    if 'is_batal' in df.columns:
        print(df.groupby("is_batal").size())
        print(f"Total is_batal=1: {df['is_batal'].sum()} (target acuan: 2830)")
    else:
        print("[WARNING] Kolom is_batal tidak ditemukan!")

    print("\n=== 3. AUDIT KURIR ('Belum ditetapkan') ===")
    kurir_col = 'nama_ekspedisi' if 'nama_ekspedisi' in df.columns else 'opsi_pengiriman'
    if kurir_col in df.columns:
        belum_set = df[df[kurir_col] == 'Belum ditetapkan']
        total_belum_set = len(belum_set)
        print(f"Total baris '{kurir_col}' = 'Belum ditetapkan': {total_belum_set} (target acuan: 765)")
        
        if 'is_batal' in df.columns and total_belum_set > 0:
            batal_count = belum_set['is_batal'].sum()
            print(f"Baris 'Belum ditetapkan' yang berstatus batal (is_batal=1): {batal_count} / {total_belum_set}")
    else:
        print(f"[WARNING] Kolom {kurir_col} tidak ditemukan!")

    print("\n=== 4. AUDIT WAKTU KOSONG ===")
    if 'Waktu Pesanan Dibuat' in df.columns:
        waktu_null = df['Waktu Pesanan Dibuat'].isna().sum()
        print(f"Jumlah Waktu Pesanan Dibuat NULL/NaT: {waktu_null} (target: 0)")

if __name__ == "__main__":
    main()