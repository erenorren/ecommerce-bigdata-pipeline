import pandas as pd

def main():
    path_gold = '/opt/airflow/data/gold/fact_orders.parquet'
    df = pd.read_parquet(path_gold)
    total = len(df)

    print("\n==================================================")
    print("      AUDIT LENGKAP GOLD LAYER (FINAL QA)         ")
    print("==================================================")

    # 1. Total Baris & Unik ID
    duplikat = df['order_id'].duplicated().sum()
    print(f"1. Total Baris            : {total} (Acuan: 20848)")
    print(f"2. Duplikat order_id      : {duplikat} (Acuan: 0)")

    # 2. Geospasial
    gagal_coords = df['lat'].isna().sum()
    print(f"3. Koordinat Kosong       : {gagal_coords} baris (Acuan: 0)")

    # 3. Target ML
    batal_count = df['is_batal'].sum()
    print(f"4. Total is_batal=1       : {batal_count} (Acuan: 2830)")

    # 4. Kurir Faktual
    kurir_belum = df[df['nama_ekspedisi'] == 'Belum ditetapkan']
    kurir_belum_batal = kurir_belum['is_batal'].sum()
    print(f"5. Kurir 'Belum ditetapkan': {len(kurir_belum)} (Acuan: 765)")
    print(f"   └─ Status Batal (1)    : {kurir_belum_batal} / {len(kurir_belum)} (100% Batal)")

    # 5. Waktu Kosong
    waktu_null = df['Waktu Pesanan Dibuat'].isna().sum()
    print(f"6. Waktu Pesanan Kosong   : {waktu_null} (Acuan: 0)")

    # 6. Fitur Kalender Nasional
    if 'Kategori' in df.columns:
        print("\n=== DISTRIBUSI PENANDA KALENDER NASIONAL ===")
        print(df['Kategori'].fillna('Hari Kerja Biasa').value_counts())

if __name__ == "__main__":
    main()