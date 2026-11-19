"""
Tabel rekomendasi kurir per wilayah (provinsi)
Febrianti, Data Analyst / ML Engineer

Cara pakai (dari folder repo):
    python analysis/rekomendasi_kurir.py

Membaca data/gold/fact_orders.parquet (atau .csv bila parquet tidak bisa dibaca).
Menghasilkan: analysis/hasil/rekomendasi_kurir.csv, performa_kurir_wilayah.csv, backtest_kurir.csv

Aturan:
- Hanya kasus kegagalan pengiriman: alasan "Pengiriman gagal" atau "Paket hilang di perjalanan".
  Pembatalan karena pembeli, belum dibayar, atau penjual TIDAK dihitung sebagai kesalahan kurir.
- Pesanan tanpa kurir ("Belum ditetapkan") dan pesanan belum final dikeluarkan.
- Wilayah = provinsi (tingkat kabupaten/kota terlalu sedikit datanya).
- Pasangan wilayah-kurir layak dinilai bila punya >= MIN_PESANAN pesanan.
- Wilayah "data memadai" bila punya >= 2 kurir yang layak dinilai (kalau hanya satu kurir, tidak ada pembanding).
- Tingkat gagal dihaluskan (shrinkage) ke perkiraan awal (tingkat wilayah x kualitas kurir secara nasional)
  agar sampel kecil tidak menipu, misalnya 0 kegagalan dari 40 pesanan tidak otomatis berarti kurir terbaik.
"""
from pathlib import Path
import numpy as np, pandas as pd

MIN_PESANAN = 30        # minimum pesanan per pasangan wilayah-kurir
SHRINK_K = 30           # kekuatan penghalusan (setara 30 pesanan dengan tingkat wilayah)
TANGGAL_PISAH = "2025-05-31"   # backtest: belajar dari data sebelum tanggal ini, uji sesudahnya

ROOT = Path(__file__).resolve().parent.parent
PARQUET, CSV = ROOT / "data/gold/fact_orders.parquet", ROOT / "data/gold/fact_orders.csv"
OUT = Path(__file__).resolve().parent / "hasil"
OUT.mkdir(exist_ok=True)

try:
    df = pd.read_parquet(PARQUET)
except Exception:
    df = pd.read_csv(CSV)
df["waktu"] = pd.to_datetime(df["waktu_pesanan_dibuat"], format="mixed")

# ---------- data dan label kegagalan kirim ----------
alasan = df.alasan_pembatalan.fillna("")
df["gagal_kirim"] = ((df.is_batal == 1) & (alasan.str.contains("Pengiriman gagal", case=False)
                                           | alasan.str.contains("Paket hilang", case=False))).astype(int)
d = df[(df.nama_ekspedisi != "Belum ditetapkan") & df.status_pesanan.isin(["Selesai", "Batal"])].copy()
print(f"pesanan dipakai: {len(d)} | kasus gagal kirim: {d.gagal_kirim.sum()} | wilayah: {d.provinsi.nunique()}")


def performa(data):
    g = data.groupby(["provinsi", "nama_ekspedisi"]).agg(
        pesanan=("order_id", "size"), gagal_kirim=("gagal_kirim", "sum"),
        rata_ongkir=("perkiraan_ongkos_kirim", "mean")).reset_index()
    wil = data.groupby("provinsi").gagal_kirim.agg(["sum", "size"])
    g["tingkat_wilayah"] = g.provinsi.map(wil["sum"] / wil["size"])
    kur = data.groupby("nama_ekspedisi").gagal_kirim.mean()
    g["tingkat_kurir_nasional"] = g.nama_ekspedisi.map(kur)
    # perkiraan awal: tingkat wilayah, disesuaikan dengan seberapa buruk/baik kurir itu secara nasional
    prior = g.tingkat_wilayah * g.tingkat_kurir_nasional / data.gagal_kirim.mean()
    g["tingkat_gagal"] = g.gagal_kirim / g.pesanan
    g["tingkat_halus"] = (g.gagal_kirim + SHRINK_K * prior) / (g.pesanan + SHRINK_K)
    g["layak"] = g.pesanan >= MIN_PESANAN
    return g


def rekomendasi(g):
    baris = []
    for prov, x in g.groupby("provinsi"):
        layak = x[x.layak]
        dominan = x.sort_values("pesanan", ascending=False).iloc[0]
        r = {"provinsi": prov, "pesanan_wilayah": int(x.pesanan.sum()), "gagal_kirim_wilayah": int(x.gagal_kirim.sum()),
             "kurir_dominan": dominan.nama_ekspedisi, "pesanan_dominan": int(dominan.pesanan),
             "tingkat_gagal_dominan": round(dominan.tingkat_gagal, 4), "rata_ongkir_dominan": round(dominan.rata_ongkir)}
        if len(layak) >= 2:
            b = layak.sort_values(["tingkat_halus", "rata_ongkir"]).iloc[0]
            r.update({"status": "Rekomendasi", "kurir_rekomendasi": b.nama_ekspedisi, "pesanan_rekomendasi": int(b.pesanan),
                      "gagal_rekomendasi": int(b.gagal_kirim), "tingkat_gagal_rekomendasi": round(b.tingkat_gagal, 4),
                      "tingkat_halus_rekomendasi": round(b.tingkat_halus, 4), "rata_ongkir_rekomendasi": round(b.rata_ongkir),
                      "sama_dengan_dominan": b.nama_ekspedisi == dominan.nama_ekspedisi})
        else:
            r.update({"status": "Data belum memadai", "kurir_rekomendasi": None})
        baris.append(r)
    return pd.DataFrame(baris).sort_values(["status", "pesanan_wilayah"], ascending=[False, False])


# ---------- tabel rekomendasi (seluruh data) ----------
g = performa(d)
tabel = rekomendasi(g)
g.round(4).to_csv(OUT / "performa_kurir_wilayah.csv", index=False)
tabel.to_csv(OUT / "rekomendasi_kurir.csv", index=False)

n_wil = len(tabel); n_ok = (tabel.status == "Rekomendasi").sum()
print(f"\nwilayah dengan data memadai (>=2 kurir dengan >= {MIN_PESANAN} pesanan): {n_ok} dari {n_wil} ({n_ok/n_wil:.0%})")
print("rekomendasi sama dengan kurir dominan:", int(tabel.sama_dengan_dominan.fillna(False).sum()), "dari", n_ok)
print(tabel[tabel.status == "Rekomendasi"][["provinsi", "pesanan_wilayah", "kurir_dominan", "tingkat_gagal_dominan",
                                           "kurir_rekomendasi", "tingkat_gagal_rekomendasi", "tingkat_halus_rekomendasi"]].to_string(index=False))

# ---------- backtesting: belajar dari masa lalu, uji pada masa depan ----------
lalu, depan = d[d.waktu <= TANGGAL_PISAH], d[d.waktu > TANGGAL_PISAH]
t_lalu = rekomendasi(performa(lalu))
bt = []
for _, r in t_lalu[t_lalu.status == "Rekomendasi"].iterrows():
    x = depan[depan.provinsi == r.provinsi]
    rek, lain = x[x.nama_ekspedisi == r.kurir_rekomendasi], x[x.nama_ekspedisi != r.kurir_rekomendasi]
    bt.append({"provinsi": r.provinsi, "kurir_rekomendasi": r.kurir_rekomendasi,
               "uji_pesanan_rekomendasi": len(rek), "uji_gagal_rekomendasi": int(rek.gagal_kirim.sum()),
               "uji_pesanan_lain": len(lain), "uji_gagal_lain": int(lain.gagal_kirim.sum())})
bt = pd.DataFrame(bt)
if len(bt):
    bt.to_csv(OUT / "backtest_kurir.csv", index=False)
    pr, gr = bt.uji_pesanan_rekomendasi.sum(), bt.uji_gagal_rekomendasi.sum()
    pl, gl = bt.uji_pesanan_lain.sum(), bt.uji_gagal_lain.sum()
    tr, tl = (gr / pr if pr else np.nan), (gl / pl if pl else np.nan)
    print(f"\nBACKTEST (belajar <= {TANGGAL_PISAH}, uji sesudahnya, {len(bt)} wilayah)")
    print(f"  semua wilayah (acuan saja, termasuk wilayah yang rekomendasinya sama dengan kurir dominan):")
    print(f"    kurir rekomendasi: {gr}/{pr} = {tr:.2%} | kurir lain: {gl}/{pl} = {tl:.2%}")
    beda = t_lalu[(t_lalu.status == "Rekomendasi") & (~t_lalu.sama_dengan_dominan.astype(bool))].provinsi
    bb = bt[bt.provinsi.isin(beda)]
    if len(bb):
        pr2, gr2, pl2, gl2 = (bb.uji_pesanan_rekomendasi.sum(), bb.uji_gagal_rekomendasi.sum(),
                              bb.uji_pesanan_lain.sum(), bb.uji_gagal_lain.sum())
        print(f"  Hanya wilayah yang rekomendasinya BERBEDA dari kurir dominan ({len(bb)} wilayah):")
        tl2 = gl2 / max(pl2, 1)
        print(f"    kurir rekomendasi: {gr2}/{pr2} = {gr2/max(pr2,1):.2%} | kurir lain: {gl2}/{pl2} = {tl2:.2%}")
        print(f"    dengan tingkat kurir lain, kurir rekomendasi diharapkan gagal {pr2*tl2:.1f} kali; yang terjadi {gr2} kali.")
        if tl2 > 0:
            print(f"    potensi penurunan tingkat gagal kirim: {(tl2 - gr2/max(pr2,1)) / tl2:.0%} (target charter >= 10%)")
        if pr2 * tl2 < 5:
            print("    PERINGATAN: kejadian yang diharapkan kurang dari 5, jadi angka ini belum bisa dipakai sebagai bukti.")
    print("  CATATAN: jumlah kasus sangat kecil, jadi angka ini tidak stabil dan bukan bukti sebab-akibat.")
else:
    print("\nBacktest tidak bisa dijalankan: tidak ada wilayah dengan data memadai pada masa belajar.")