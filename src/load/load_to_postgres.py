from pyspark.sql import SparkSession

def main():
    # Inisialisasi Spark dengan driver PostgreSQL
    spark = SparkSession.builder \
        .appName("Ecommerce-Load-Postgres") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    gold_path = "/opt/airflow/data/gold/fact_orders.parquet"
    print(f"[INFO] Membaca data Gold Layer dari: {gold_path}")
    
    df_gold = spark.read.parquet(gold_path)
    print(f"  └─ Total baris yang akan dimuat: {df_gold.count()}")

    # Konfigurasi Koneksi PostgreSQL
    # Karena ini dijalankan di dalam container Airflow, kita gunakan nama service 'postgres'
    db_url = "jdbc:postgresql://postgres:5432/ecommerce_dw"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    
    table_name = "fact_orders"

    print(f"[INFO] Memuat data ke tabel '{table_name}' di PostgreSQL...")
    
    try:
        df_gold.write.jdbc(
            url=db_url,
            table=table_name,
            mode="overwrite", # Menimpa tabel jika sudah ada
            properties=db_properties
        )
        print("[SUCCESS] Data berhasil dimuat ke PostgreSQL!")
    except Exception as e:
        print(f"[ERROR] Gagal memuat data ke PostgreSQL: {e}")

    spark.stop()

if __name__ == "__main__":
    main()