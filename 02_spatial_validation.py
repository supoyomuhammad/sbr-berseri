import polars as pl
import geopandas as gpd
import time

def main():
    start_time = time.time()
    print("Membaca data Parquet hasil ekstraksi dan file peta GeoJSON...")

    # 1. Load Data
    # Menggunakan file parquet hasil script pertama
    df = pl.read_parquet("input-2026_02_27_cleaned.parquet")
    regions = gpd.read_file("provinsi_lampung.geojson")

    # 2. Siapkan data wilayah (Kita hanya butuh iddesa/idsls untuk deteksi overlay)
    regions['iddesa'] = regions['idsls'].astype(str).str[:10]
    regions = regions[['iddesa', 'geometry']]

    print("Memfilter titik koordinat valid untuk dievaluasi...")
    # 3. Filter data yang wajib dicek (gcs_result = 1 dan latlong valid)
    df_to_check = df.filter(
        (pl.col("gcs_result") == "1") & (pl.col("latlong_status_gc") == "valid")
    ).with_columns(
        lon=pl.col("longitude_gc").cast(pl.Float64),
        lat=pl.col("latitude_gc").cast(pl.Float64)
    )

    print("Memulai proses Overlay Spasial (Point in Polygon)...")
    # 4. Konversi ke GeoDataFrame dan lakukan Spatial Join
    gdf_points = gpd.GeoDataFrame(
        df_to_check.to_pandas(),
        geometry=gpd.points_from_xy(df_to_check["lon"], df_to_check["lat"]),
        crs="EPSG:4326"
    )

    # sjoin 'within' mengecek apakah titik jatuh di dalam area provinsi/kabupaten
    joined_gdf = gpd.sjoin(gdf_points, regions, how="left", predicate="within")

    # 5. Ekstrak hasil spasial kembali ke Polars
    # Jika titik tidak jatuh di polygon mana pun, kolom iddesa akan bernilai Null
    spatial_result = pl.from_pandas(joined_gdf.drop(columns=["geometry", "index_right"])).select([
        "idsbr",
        pl.col("iddesa").is_null().alias("is_titik_diluar_prov")
    ])

    print("Membentuk vflag_006 ke dalam data utama...")
    # 6. Gabungkan kembali ke data utama dan terapkan aturan bisnis vflag_006
    df_final = df.join(spatial_result, on="idsbr", how="left")

    df_final = df_final.with_columns(
        vflag_006=pl.when(
            (pl.col("gcs_result") == "1") & (pl.col("is_titik_diluar_prov") == True)
        ).then(
            pl.lit("🙅: kode wilayah gc berada diluar kab/kot & tidak sesuai dengan koordinat di peta wilkerstat")
        ).when(
            (pl.col("gcs_result") == "1") & (pl.col("is_titik_diluar_prov").is_null())
        ).then(
            pl.lit("🙅: latlong_status_gc invalid")
        ).otherwise(
            pl.lit("sesuai")
        )
    ).drop("is_titik_diluar_prov") # Bersihkan kolom sementara

    # 7. Simpan hasil akhir
    output_file = "data_with_vflag006.parquet"
    df_final.write_parquet(output_file)

    print(f"Selesai! Data final dengan vflag_006 tersimpan di: {output_file}")
    print(f"Waktu eksekusi: {time.time() - start_time:.2f} detik")

if __name__ == "__main__":
    main()