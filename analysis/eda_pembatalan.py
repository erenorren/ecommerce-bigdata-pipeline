"""
EDA awal pembatalan pesanan (Febrianti, Data Analyst / ML Engineer)

Cara pakai:
  1. Ubah BASE ke folder yang berisi Data 2023, Data 2024, Data 2025,
     Kalender_Nasional_dan_Promo_2022_2025.xlsx, dan lat_long_kota_kab.csv.
  2. pip install pandas openpyxl scipy
  3. python eda_febrianti.py

Hanya file di dalam folder "Data 2023/2024/2025" yang dibaca. File dengan nama
sama di luar folder itu (versi lama) sengaja diabaikan.
"""
import re
from pathlib import Path

import pandas as pd
from scipy.stats import chi2_contingency

BASE = Path(".")  # <-- ganti
pd.set_option("display.width", 200, "display.max_colwidth", 70, "display.max_rows", 60)


def judul(t):
    print("\n" + "=" * 8, t, "=" * 8)


# ---------------------------------------------------------------- 1. muat data
files = sorted(p for y in ("Data 2023", "Data 2024", "Data 2025") for p in (BASE / y).glob("*.xlsx"))
print(len(files), "file ditemukan (harus 24)")
frames = []
for f in files:
    d = pd.read_excel(f)
    d["sumber_file"] = f.name
    frames.append(d)

kolom_per_file = {}
for d in frames:
    k = tuple(c for c in d.columns if c != "sumber_file")
    kolom_per_file.setdefault(k, []).append(d.sumber_file.iloc[0])
judul("Konsistensi kolom antar file")
umum = max(kolom_per_file, key=lambda k: len(kolom_per_file[k]))
for k, v in kolom_per_file.items():
    if k != umum:
        print("File berbeda:", v, "| kolom hilang:", set(umum) - set(k), "| tambahan:", set(k) - set(umum))
print(len(umum), "kolom pada", len(kolom_per_file[umum]), "file mayoritas")

df = pd.concat(frames, ignore_index=True)
judul("Ukuran dan kualitas dasar")
print("baris, kolom (termasuk sumber_file):", df.shape)
print("duplikat order_id:", df.order_id.duplicated().sum())
print("nilai kosong:\n", df.isna().sum()[lambda s: s > 0])

# ---------------------------------------------------------------- 2. label & fitur dasar
df["is_batal"] = (df["Status Pesanan"] == "Batal").astype(int)
df["kurir"] = df["Opsi Pengiriman"].str.split("-").str[-1].str.strip()
# Tanpa kurir = opsi pengiriman tidak memuat nama ekspedisi (sama dengan aturan di pipeline Sheren)
df["tanpa_kurir"] = (~df["Opsi Pengiriman"].str.lower().str.contains(
    r"spx|shopee|j&t|jnt|jne|go-?send|grab", regex=True)).astype(int)
df["status_final"] = df["Status Pesanan"].isin(["Selesai", "Batal"]).astype(int)
df["t"] = pd.to_datetime(df["Waktu Pesanan Dibuat"].map(
    lambda x: x if (pd.isna(x) or not isinstance(x, str)) else pd.Timestamp(x)))

judul("Proporsi batal")
print("tingkat batal:", round(df.is_batal.mean(), 4), "| batal:", df.is_batal.sum(), "dari", len(df))
print("pesanan tanpa kurir:", df.tanpa_kurir.sum(), "| tingkat batal mereka:", df[df.tanpa_kurir == 1].is_batal.mean())
print("pesanan belum final:", (df.status_final == 0).sum())
print("pesanan tanpa waktu:", df.t.isna().sum(), df[df.t.isna()].sumber_file.value_counts().to_dict())

# ---------------------------------------------------------------- 3. kebocoran
judul("Cek kolom bocor: nilai pada pesanan batal vs tidak batal")
for c in ["Total Pembayaran", "Ongkos Kirim Dibayar oleh Pembeli", "Estimasi Potongan Biaya Pengiriman",
          "total_returned_qty", "Perkiraan Ongkos Kirim", "Total Diskon"]:
    b, n = df[df.is_batal == 1][c], df[df.is_batal == 0][c]
    print(f"{c:38s} batal: rata-rata {b.mean():>10.1f}, nol {100*(b==0).mean():5.1f}% | "
          f"tidak batal: rata-rata {n.mean():>10.1f}, nol {100*(n==0).mean():5.1f}%")

# ---------------------------------------------------------------- 4. pola (tanpa pesanan tanpa kurir)
d = df[df.tanpa_kurir == 0].copy()
print("\nTingkat batal setelah pesanan tanpa kurir dikeluarkan:", round(d.is_batal.mean(), 4), "dari", len(d), "baris")


def tabel(kolom, minimal=50, n=12, data=d):
    r = data.groupby(kolom).is_batal.agg(jumlah="size", batal="sum", tingkat="mean")
    r = r[r.jumlah >= minimal].sort_values("tingkat", ascending=False)
    judul(f"Tingkat batal per {kolom}")
    print(r.head(n).round(3))


tabel("Metode Pembayaran", minimal=1)
tabel("kurir", minimal=30)
tabel("Provinsi", minimal=100)
tabel("Kota/Kabupaten", minimal=100)
tabel("product_categories", minimal=100, n=10)
d["cod"] = d["Metode Pembayaran"].str.startswith("COD")
tabel("cod", minimal=1)

judul("Tren bulanan (semua pesanan)")
print(df.groupby(df.t.dt.to_period("M")).is_batal.agg(["size", "mean"]).round(3))

# ---------------------------------------------------------------- 5. alasan pembatalan
def kategori_alasan(s):
    if pd.isna(s):
        return None
    s = s.lower()
    if "belum dibayar" in s:
        return "Belum dibayar"
    if "pengiriman gagal" in s or "hilang" in s or "paket" in s:
        return "Kegagalan pengiriman"
    if "penjual" in s:
        return "Faktor penjual"
    if "pembeli" in s:
        return "Keputusan pembeli"
    return "Lainnya"


judul("Kategori alasan pembatalan")
print(df[df.is_batal == 1]["Alasan Pembatalan"].map(kategori_alasan).value_counts())

# ---------------------------------------------------------------- 6. kalender
kal = pd.read_excel(BASE / "Kalender_Nasional_dan_Promo_2022_2025.xlsx")
kal["Tanggal"] = pd.to_datetime(kal.Tanggal)
w = d[d.t.notna()].copy()
w["tgl"] = w.t.dt.normalize()
judul(f"Efek hari libur (n={len(w)}; baris tanpa waktu dan tanpa kurir dikeluarkan)")
for kat in kal.Kategori.unique():
    w["f"] = w.tgl.isin(set(kal[kal.Kategori == kat].Tanggal)).astype(int)
    p = chi2_contingency(pd.crosstab(w.f, w.is_batal))[1]
    print(kat, w.groupby("f").is_batal.mean().round(3).to_dict(), "p =", round(p, 4))

# ---------------------------------------------------------------- 7. wilayah vs koordinat
ll = pd.read_csv(BASE / "lat_long_kota_kab.csv")
norm = lambda s: re.sub(r"^KABUPATEN ", "KAB. ", s.upper().strip())
nama = df["Kota/Kabupaten"].map(norm)
belum = sorted(set(nama) - set(ll.name.map(norm)))
judul("Nama wilayah yang belum cocok dengan file koordinat")
print(len(belum), "nama,", nama.isin(belum).sum(), "baris")
print(belum)

df.to_csv("transaksi_gabungan.csv", index=False)
print("\nSelesai. Data gabungan disimpan ke transaksi_gabungan.csv")