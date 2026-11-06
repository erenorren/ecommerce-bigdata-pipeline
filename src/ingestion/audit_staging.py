import pandas as pd

def main():
    # Membaca seluruh file Parquet di folder staging
    df = pd.read_parquet('/opt/airflow/data/staging/')
    total = len(df)
    
    # Deteksi nama kolom ID (No. Pesanan atau order_id)
    id_col = 'No. Pesanan' if 'No. Pesanan' in df.columns else 'order_id'
    is_unique = df[id_col].is_unique if id_col in df.columns else False

    print('\n--- AUDIT STAGING (TAHAP A & B) ---')
    print(f'Total baris: {total} (harus 20848)')
    print(f'ID Pesanan unik: {is_unique} (harus True)')
    print(f'Kolom source_file ada: {"source_file" in df.columns}')

    if "source_file" in df.columns:
        print('\nSebaran baris pada 5 file pertama (cek acak):')
        print(df.groupby('source_file').size().head())

if __name__ == "__main__":
    main()