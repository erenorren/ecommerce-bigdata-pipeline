import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, to_date, date_format, dayofweek, 
    when, year, month, coalesce, lit, collect_set, concat_ws
)

def main():
    spark = SparkSession.builder \
        .appName("Ecommerce-Transform-Gold") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    silver_orders_path = "/opt/airflow/data/curated/orders_cleaned.parquet"
    geo_path = "/opt/airflow/reference/koordinat_wilayah.csv"
    cal_path = "/opt/airflow/reference/kalender_nasional.csv"
    gold_dir = "/opt/airflow/data/gold/fact_orders.parquet"

    os.makedirs("/opt/airflow/data/gold", exist_ok=True)

    print("[INFO] Membaca data dari Silver Layer & Reference Files...")
    df_orders = spark.read.parquet(silver_orders_path)
    df_geo = spark.read.option("header", "true").csv(geo_path)
    df_cal = spark.read.option("header", "true").csv(cal_path)

    # 1. DEDUPLIKASI GEO DATA (1 Wilayah = 1 Pasang Koordinat)
    df_geo_clean = df_geo.drop("_c0") \
        .withColumnRenamed("id", "kode_wilayah") \
        .withColumnRenamed("foreign", "kode_provinsi") \
        .withColumnRenamed("name", "nama_wilayah_koordinat") \
        .withColumn("match_key", lower(trim(col("nama_wilayah_koordinat")))) \
        .withColumn("lat", col("lat").cast("double")) \
        .withColumn("long", col("long").cast("double")) \
        .dropDuplicates(["match_key"])

    # 2. JOIN GEOSPASIAL
    df_joined = df_orders.join(
        df_geo_clean, 
        df_orders["Kota/Kabupaten_Clean"] == df_geo_clean["match_key"], 
        "left"
    ).drop("match_key")

    # 3. AGREGASI KALENDER NASIONAL PER TANGGAL (Mencegah Duplikasi Row)
    df_cal_agg = df_cal \
        .withColumn("tanggal_kalender", to_date(col("Tanggal"))) \
        .groupBy("tanggal_kalender") \
        .agg(
            concat_ws(" | ", collect_set("Kategori")).alias("kategori_raw"),
            concat_ws(" | ", collect_set("Keterangan")).alias("nama_hari_libur")
        )

    # Fitur Temporal Order
    df_joined = df_joined.withColumn("tanggal_pesanan", to_date(col("Waktu Pesanan Dibuat")))

    # JOIN KALENDER NASIONAL
    df_joined = df_joined.join(
        df_cal_agg, 
        df_joined["tanggal_pesanan"] == df_cal_agg["tanggal_kalender"], 
        "left"
    ).drop("tanggal_kalender")

    # Fitur Hari & Akhir Pekan
    df_joined = df_joined.withColumn("hari_dalam_minggu", date_format(col("tanggal_pesanan"), "EEEE"))
    df_joined = df_joined.withColumn(
        "is_akhir_pekan",
        when(dayofweek(col("tanggal_pesanan")).isin(1, 7), 1).otherwise(0)
    )

    # Indikator Boolean Kalender Fleksibel
    cat_lower = lower(coalesce(col("kategori_raw"), lit("")))
    df_joined = df_joined.withColumn("is_libur_nasional", when(cat_lower.contains("libur"), 1).otherwise(0))
    df_joined = df_joined.withColumn("is_cuti_bersama", when(cat_lower.contains("cuti"), 1).otherwise(0))
    df_joined = df_joined.withColumn("is_event_promo", when(cat_lower.contains("promo"), 1).otherwise(0))
    df_joined = df_joined.withColumn("is_ramadan", when(cat_lower.contains("ramadan"), 1).otherwise(0))

    # Kategori Hari Tunggal Terstandarisasi
    df_joined = df_joined.withColumn(
        "kategori_hari",
        when(col("is_libur_nasional") == 1, "Hari Libur Nasional")
        .when(col("is_cuti_bersama") == 1, "Cuti Bersama")
        .when(col("is_event_promo") == 1, "Event Promo")
        .when(col("is_akhir_pekan") == 1, "Akhir Pekan")
        .otherwise("Hari Biasa")
    )

    df_joined = df_joined.withColumn("tahun_pesanan", year(col("tanggal_pesanan")))
    df_joined = df_joined.withColumn("bulan_pesanan", month(col("tanggal_pesanan")))

    # 4. STANDARISASI NAMA KOLOM KE SNAKE_CASE
    df_final = df_joined \
        .withColumnRenamed("Waktu Pesanan Dibuat", "waktu_pesanan_dibuat") \
        .withColumnRenamed("Total Diskon", "total_diskon") \
        .withColumnRenamed("Total Pembayaran", "total_pembayaran") \
        .withColumnRenamed("Status Pesanan", "status_pesanan") \
        .withColumnRenamed("Alasan Pembatalan", "alasan_pembatalan") \
        .withColumnRenamed("Metode Pembayaran", "metode_pembayaran") \
        .withColumnRenamed("Kota/Kabupaten", "kota_kabupaten") \
        .withColumnRenamed("Provinsi", "provinsi") \
        .withColumnRenamed("Perkiraan Ongkos Kirim", "perkiraan_ongkos_kirim") \
        .withColumnRenamed("Kota/Kabupaten_Clean", "kota_kabupaten_clean") \
        .drop("Tahun", "Bulan", "Kategori", "kategori_raw")

    print("[INFO] Menyimpan data gabungan ke Gold Layer...")
    df_final.coalesce(1).write.mode("overwrite").parquet(gold_dir)
    print(f"  └─ Total pesanan setelah JOIN: {df_final.count()}")
    print(f"[SUCCESS] Data gabungan berhasil disimpan di: {gold_dir}")

    spark.stop()

if __name__ == "__main__":
    main()