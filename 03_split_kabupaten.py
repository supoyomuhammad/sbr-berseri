import polars as pl
import os
import time

def main():
    start_time = time.time()
    print("Membaca data final...")
    
    input_file = "data_with_vflag006.parquet"
    df = pl.read_parquet(input_file)

    print("Memfilter hanya data anomali spasial...")
    df_anomali = df.filter(pl.col("vflag_006") != "sesuai")
    
    if df_anomali.is_empty():
        print("Luar biasa! Tidak ada data anomali yang ditemukan. Semua titik koordinat sesuai.")
        return

    output_dir = "./kabupaten_split_anomali"
    os.makedirs(output_dir, exist_ok=True)

    print(f"Ditemukan {df_anomali.height} baris anomali. Mempartisi data...")
    
    # Menggunakan perulangan biasa agar error tidak disembunyikan
    for name, group_df in df_anomali.partition_by("kdkab", as_dict=True).items():
        # Memastikan kode kabupaten terbaca dengan benar terlepas dari versi Polars
        kodekab = name[0] if isinstance(name, tuple) else name
        
        if not kodekab:
            continue

        file_path = os.path.join(output_dir, f"data_anomali_18{kodekab}.xlsx")
        
        # Proses menyimpan ke Excel
        group_df.write_excel(file_path)
        print(f" -> Tersimpan: {file_path} ({group_df.height} baris)")

    print("-" * 30)
    print(f"Selesai! Semua file anomali berhasil di-generate di folder '{output_dir}'.")
    print(f"Waktu eksekusi total: {time.time() - start_time:.2f} detik")

if __name__ == "__main__":
    main()