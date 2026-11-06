import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

def main():
    spark = SparkSession.builder \
        .appName("Ecommerce-Transform-Geo") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    ref_path = "/opt/airflow/reference/koordinat_wilayah.csv"
    output_path = "/opt/airflow/data/curated/geo_cleaned.parquet"
    os.makedirs("/opt/airflow/data/curated", exist_ok=True)

    print("[INFO] Membaca data koordinat wilayah...")
    df_geo = spark.read.option("header", "true").csv(ref_path)

    # Memakai kolom 'name' dari CSV aslinya
    # Normalisasi hanya menjadi huruf kecil dan hapus spasi berlebih
    # (Selaras dengan tabel order, tidak membuang prefiks Kota/Kab)
    df_clean = df_geo.withColumn("Kota/Kabupaten_Clean", lower(trim(col("name"))))

    df_clean.write.mode("overwrite").parquet(output_path)
    print(f"[SUCCESS] Data koordinat wilayah bersih tersimpan di: {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()