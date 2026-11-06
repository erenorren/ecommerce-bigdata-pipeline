import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def main():
    spark = SparkSession.builder \
        .appName("Ecommerce-Transform-Calendar") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    ref_path = "/opt/airflow/reference/kalender_nasional.csv"
    output_path = "/opt/airflow/data/curated/calendar_cleaned.parquet"
    os.makedirs("/opt/airflow/data/curated", exist_ok=True)

    print("[INFO] Membaca data kalender nasional...")
    df_cal = spark.read.option("header", "true").csv(ref_path)

    # Cast ke tipe Date dan buang duplikasi tanggal jika ada
    df_clean = df_cal.withColumn("Tanggal", to_date(col("Tanggal")))
    df_clean = df_clean.dropDuplicates(["Tanggal"])

    df_clean.write.mode("overwrite").parquet(output_path)
    print(f"[SUCCESS] Kalender nasional bersih tersimpan di: {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()