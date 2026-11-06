# 🛒 E-Commerce Cancellation Prediction Pipeline

## Deskripsi Proyek
Proyek ini merupakan **Data Engineering & Machine Learning Pipeline** end-to-end untuk menganalisis dan memprediksi risiko pembatalan pesanan (churn/cancellation) pelanggan pada platform *e-commerce*. 

Pipeline dirancang mengikuti arsitektur **Medallion (Bronze -> Silver -> Gold)**, diorkestrasi secara otomatis menggunakan **Apache Airflow**, diproses secara terdistribusi menggunakan **Apache Spark (PySpark)**, dan bermuara pada **PostgreSQL** untuk divisualisasikan menggunakan Power BI / Looker Studio.

---

## 🏗️ Arsitektur Data
*   **Ingestion (Bronze Layer):** Ekstraksi 24 file `.xlsx` bulanan dan konversi ke `.parquet` menggunakan Pandas/Openpyxl.
*   **Transformation (Silver Layer):** Pembersihan dataset utama, ekstraksi fitur (Waktu, Kurir, Layanan), dan penggabungan (*join*) terdistribusi dengan data penunjang (Kalender Nasional & Koordinat Wilayah) menggunakan **PySpark**.
*   **Load & Serving (Gold Layer):** Data bersih dan tervalidasi dimuat ke **PostgreSQL**.
*   **Orchestration:** **Apache Airflow** digunakan untuk penjadwalan DAG bulanan dengan fitur *catchup* aktif untuk *backfilling* data historis.
*   **Machine Learning:** Model skoring risiko pembatalan (0-100) dilatih menggunakan fitur terfilter (menghindari *data leakage*).

---

## 📂 Struktur Repositori Terpilih
- `dags/` : Definisi alur kerja bulanan Airflow.
- `src/` : Kumpulan skrip ETL PySpark yang dieksekusi oleh DAG.
- `reference/` : Data statis pendukung (*lookup tables*) berukuran kecil.
- `docs/` : Dokumentasi teknis proyek, ERD, dan kamus data.
- `data/` : Folder lokal pengganti Data Lake (Raw, Staging, Curated). **(TIDAK DI-COMMIT)**.

---

## 🚀 Panduan Menjalankan Sistem Lokal (Docker)

### Prasyarat
- Git
- Docker Desktop (alokasi RAM min. 8GB disarankan)
- VS Code (dengan ekstensi Python, Docker, WSL)

### 1. Inisialisasi Environment
Clone repositori dan siapkan file environment:
\`\`\`bash
git clone <url-repo>
cd ecommerce-cancellation-pipeline
cp .env.example .env
\`\`\`
*(Catatan: Jangan pernah commit file `.env`)*

### 2. Siapkan Data Mentah
Letakkan 24 file mentah e-commerce bulanan `.xlsx` ke dalam folder `data/raw/`.

### 3. Build & Jalankan Docker
\`\`\`bash
# Build image khusus (dengan PySpark dan Java JRE 17 terintegrasi)
docker compose build

# Inisialisasi basis data Airflow
docker compose up airflow-init

# Jalankan seluruh layanan (Airflow & PostgreSQL Warehouse)
docker compose up -d
\`\`\`

### 4. Akses Layanan
*   **Apache Airflow:** Buka browser di `http://localhost:8080` (Kredensial: *airflow / airflow*)
*   **PostgreSQL Warehouse:** Akses via DBeaver pada `localhost:5433` (Database: *ecommerce*, User/Pass: *sesuai file .env*)

---

## Tim Pengembang
1. **Kharizma Rizkiah** - Project Manager & Data Architect
2. **Regisha Sheren** - Data Engineer (Ingestion, Spark Pipeline, Airflow Orchestration)
3. **Febrianti Khumairoh** - Data Analyst & ML Engineer (Feature Engineering, Modeling, Dashboard)