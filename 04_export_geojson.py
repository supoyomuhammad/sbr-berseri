import polars as pl
import geopandas as gpd

def main():
    print("Membaca data anomali...")
    df = pl.read_parquet("data_with_vflag006.parquet")

    # Filter khusus data anomali
    df_anomali = df.filter(pl.col("vflag_006") != "sesuai")

    print("Membersihkan dan mengonversi format koordinat...")
    # Ubah koma menjadi titik, lalu konversi ke Float. 
    # strict=False memastikan data rusak tidak membuat skrip crash.
    df_anomali = df_anomali.with_columns(
        lon=pl.col("longitude_gc").str.replace(",", ".").cast(pl.Float64, strict=False),
        lat=pl.col("latitude_gc").str.replace(",", ".").cast(pl.Float64, strict=False)
    ).drop_nulls(subset=["lon", "lat"]) # Buang data yang tidak punya koordinat valid

    print("Mengonversi ke GeoJSON...")
    # Ubah ke GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df_anomali.to_pandas(),
        geometry=gpd.points_from_xy(df_anomali["lon"], df_anomali["lat"]),
        crs="EPSG:4326"
    )

    # Simpan sebagai file GeoJSON
    output_file = "anomali_titik.geojson"
    gdf.to_file(output_file, driver="GeoJSON")
    print(f"Selesai! Data diekspor ke {output_file}")

if __name__ == "__main__":
    main()