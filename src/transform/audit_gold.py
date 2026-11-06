import pandas as pd

def main():
    df = pd.read_parquet('/opt/airflow/data/gold/fact_orders.parquet')
    total = len(df)
    gagal = df['lat'].isna().sum()

    duplikat = df['order_id'].duplicated().sum() if 'order_id' in df.columns else 'Kolom ID tidak ditemukan'

    print('\n--- AUDIT HASIL JOIN GOLD LAYER ---')
    print(f'Total baris: {total} (harus 20848)')
    print(f'order_id duplikat: {duplikat} (harus 0)')
    print(f'Koordinat kosong: {gagal} baris ({(gagal/total)*100:.2f}%)')

    print('\n--- TOP 20 WILAYAH GAGAL JOIN ---')
    if gagal > 0:
        kota_col = 'Kota/Kabupaten_Clean' if 'Kota/Kabupaten_Clean' in df.columns else 'Kota/Kabupaten'
        print(df[df['lat'].isna()][kota_col].value_counts().head(20))
    else:
        print('Semua wilayah berhasil di-join!')

if __name__ == "__main__":
    main()