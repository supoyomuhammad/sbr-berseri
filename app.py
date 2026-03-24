import streamlit as st
import polars as pl
import geopandas as gpd
import folium
from streamlit_folium import st_folium

# Konfigurasi Halaman (Lebar penuh seperti desain mockup)
st.set_page_config(page_title="Cek Koordinat SBR", layout="wide", page_icon="🗺️")

# --- 1. LOAD DATA (Diberi cache agar loadingnya sangat cepat) ---
@st.cache_data
def load_data():
    # Baca data utama dan ambil hanya yang anomali
    df = pl.read_parquet("data_with_vflag006.parquet")
    df_anomali = df.filter(pl.col("vflag_006") != "sesuai")
    
    # Baca peta geojson
    gdf = gpd.read_file("provinsi_lampung.geojson")
    return df, df_anomali, gdf

df_all, df_anomali, gdf_lampung = load_data()

# --- 2. HEADER & MENU (Sesuai mockup) ---
st.title("🗺️ Cek Koordinat SBR")
st.markdown("---")

# Membuat 3 Menu/Tab
tab_dashboard, tab_rekap, tab_pengguna = st.tabs(["📊 Dashboard", "📝 Rekap", "👤 Per Pengguna"])

# --- FUNGSI BANTUAN UNTUK MENGGAMBAR PETA ---
def draw_map(df_points, gdf_poly):
    # Titik tengah default (Lampung)
    m = folium.Map(location=[-5.4254, 105.2667], zoom_start=8, tiles="CartoDB positron")
    
    # Tambahkan polygon batas desa/kecamatan
    if not gdf_poly.empty:
        folium.GeoJson(
            gdf_poly,
            style_function=lambda feature: {
                'fillColor': '#3498db', 'color': '#2980b9', 'weight': 1, 'fillOpacity': 0.1
            }
        ).add_to(m)

    # Tambahkan titik anomali
    for row in df_points.iter_rows(named=True):
        try:
            lat = float(row["latitude_gc"].replace(",", "."))
            lon = float(row["longitude_gc"].replace(",", "."))
            
            popup_html = f"""
            <b>{row['nama_usaha']}</b><br>
            ID: {row['idsbr']}<br>
            Petugas: {row['gc_username']}<br>
            Error: {row['vflag_006']}
            """
            
            folium.CircleMarker(
                location=[lat, lon],
                radius=5,
                color="red",
                fill=True,
                fill_color="red",
                fill_opacity=0.7,
                tooltip=row["nama_usaha"],
                popup=folium.Popup(popup_html, max_width=300)
            ).add_to(m)
        except:
            continue # Abaikan jika lat/lon rusak parah
            
    return m

# ==========================================
# TAB 1: DASHBOARD
# ==========================================
with tab_dashboard:
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    
    # Filter Berjenjang (Cascading)
    with col1:
        list_kab = ["Semua"] + df_anomali["nmkab"].drop_nulls().unique().to_list()
        sel_kab = st.selectbox("KABUPATEN/KOTA", list_kab, key="dash_kab")
    
    with col2:
        df_filter_kab = df_anomali if sel_kab == "Semua" else df_anomali.filter(pl.col("nmkab") == sel_kab)
        list_kec = ["Semua"] + df_filter_kab["nmkec"].drop_nulls().unique().to_list()
        sel_kec = st.selectbox("KECAMATAN", list_kec, key="dash_kec")
        
    with col3:
        df_filter_kec = df_filter_kab if sel_kec == "Semua" else df_filter_kab.filter(pl.col("nmkec") == sel_kec)
        list_desa = ["Semua"] + df_filter_kec["nmdesa"].drop_nulls().unique().to_list()
        sel_desa = st.selectbox("DESA", list_desa, key="dash_desa")
        
    with col4:
        st.write("") # Spacer
        st.write("")
        btn_cek = st.button("🎯 Cek Titik", use_container_width=True, key="dash_btn")

    # Logika Filter & Tampil Peta
    df_plot = df_filter_kec if sel_desa == "Semua" else df_filter_kec.filter(pl.col("nmdesa") == sel_desa)
    
    # Tampilkan Peta
    st.markdown("### Peta Sebaran Anomali")
    map_dash = draw_map(df_plot, gdf_lampung)
    st_folium(map_dash, width="100%", height=500, returned_objects=[])


# ==========================================
# TAB 2: REKAP
# ==========================================
with tab_rekap:
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        rekap_kab = st.selectbox("Filter Kabupaten", ["Semua"] + df_anomali["nmkab"].drop_nulls().unique().to_list(), key="rek_kab")
    with col_r2:
        rekap_user = st.selectbox("Filter Petugas (Username)", ["Semua"] + df_anomali["gc_username"].drop_nulls().unique().to_list(), key="rek_user")

    # Terapkan filter rekap
    df_rekap = df_anomali
    if rekap_kab != "Semua":
        df_rekap = df_rekap.filter(pl.col("nmkab") == rekap_kab)
    if rekap_user != "Semua":
        df_rekap = df_rekap.filter(pl.col("gc_username") == rekap_user)

    # Pilih kolom yang disajikan sesuai permintaan
    kolom_tampil = ["nmkab", "nmkec", "nmdesa", "idsbr", "latitude_gc", "longitude_gc", "gc_username", "vflag_006"]
    
    st.dataframe(df_rekap.select(kolom_tampil).to_pandas(), use_container_width=True, hide_index=True)


# ==========================================
# TAB 3: PER PENGGUNA
# ==========================================
with tab_pengguna:
    col_p1, col_p2, col_p3, col_p4 = st.columns(4)
    
    with col_p1:
        sel_p_kab = st.selectbox("KABUPATEN/KOTA", ["Semua"] + df_anomali["nmkab"].drop_nulls().unique().to_list(), key="p_kab")
    with col_p2:
        sel_p_kec = st.selectbox("KECAMATAN", ["Semua"] + df_anomali["nmkec"].drop_nulls().unique().to_list(), key="p_kec")
    with col_p3:
        sel_p_desa = st.selectbox("DESA", ["Semua"] + df_anomali["nmdesa"].drop_nulls().unique().to_list(), key="p_desa")
    with col_p4:
        sel_p_user = st.selectbox("USERNAME PETUGAS", ["Semua"] + df_anomali["gc_username"].drop_nulls().unique().to_list(), key="p_user")

    # Filter logika untuk peta per pengguna
    df_plot_user = df_anomali
    if sel_p_kab != "Semua": df_plot_user = df_plot_user.filter(pl.col("nmkab") == sel_p_kab)
    if sel_p_kec != "Semua": df_plot_user = df_plot_user.filter(pl.col("nmkec") == sel_p_kec)
    if sel_p_desa != "Semua": df_plot_user = df_plot_user.filter(pl.col("nmdesa") == sel_p_desa)
    if sel_p_user != "Semua": df_plot_user = df_plot_user.filter(pl.col("gc_username") == sel_p_user)

    st.markdown(f"### Peta Evaluasi Petugas")
    map_user = draw_map(df_plot_user, gdf_lampung)
    st_folium(map_user, width="100%", height=500, returned_objects=[])