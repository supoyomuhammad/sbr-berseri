import duckdb
import polars as pl
import time

def main():
    print("Memulai proses ekstraksi dan penggabungan data...")
    start_time = time.time()

    # 1 & 2. Mendefinisikan kolom yang spesifik untuk diambil
    columns_to_keep = [
        "idsbr", "nama_usaha", "alamat_usaha", "kode_wilayah", "kdprov", "kdkab", "kdkec", 
        "kddesa", "nmprov", "nmkab", "nmkec", "nmdesa", "perusahaan_id", "status_perusahaan", 
        "skor_kalo", "kegiatan_usaha", "rank_nama", "rank_alamat", "history_ref_profiling_id", 
        "skala_usaha", "sumber_data", "latitude", "longitude", "latlong_status", "gcid", 
        "gcs_result", "allow_cancel", "allow_edit", "allow_flagging", "latitude_gc", 
        "longitude_gc", "latlong_status_gc", "gc_username"
    ]

    # Menggabungkan nama kolom menjadi satu string untuk query SQL
    select_clause = ", ".join(columns_to_keep)

    # Menggunakan wildcard (*.json) agar DuckDB membaca semua file JSON di folder tersebut sekaligus.
    # Union by name memastikan kolom tetap sinkron meskipun urutan di JSON berbeda.
    query = f"""
        SELECT {select_clause} 
        FROM read_json_auto('./*.json', union_by_name=true)
    """
    
    # Eksekusi query dan konversi langsung ke Polars DataFrame
    df = duckdb.sql(query).pl()

    # 3. Mengonversi semua kolom menjadi tipe String
    df_cleaned = df.with_columns(pl.all().cast(pl.String))

    # 4. Menyimpan data ke format Parquet untuk efisiensi pipeline selanjutnya
    output_file = "./input-2026_02_27_cleaned.parquet"
    df_cleaned.write_parquet(output_file)

    end_time = time.time()
    print(f"Selesai! Data berhasil disimpan ke: {output_file}")
    print(f"Total baris: {df_cleaned.height}")
    print(f"Waktu eksekusi: {end_time - start_time:.2f} detik")

if __name__ == "__main__":
    main()