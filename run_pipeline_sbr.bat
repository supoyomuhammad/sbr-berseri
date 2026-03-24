@echo off
echo ===================================================
echo       PIPELINE ETL DATA GROUND CHECK SBR
echo ===================================================
echo.

echo [1/4] Menjalankan Ekstraksi Data JSON...
python 01_extract_data.py
if %errorlevel% neq 0 goto error

echo.
echo [2/4] Menjalankan Validasi Spasial (vflag_006)...
python 02_spatial_validation.py
if %errorlevel% neq 0 goto error

echo.
echo [3/4] Menjalankan Pemecahan Data per Kabupaten...
python 03_split_kabupaten.py
if %errorlevel% neq 0 goto error

echo.
echo [4/4] Mengekspor Data ke GeoJSON untuk Web...
python 04_export_geojson.py
if %errorlevel% neq 0 goto error

echo.
echo ===================================================
echo PIPELINE LOKAL SELESAI! Mengunggah ke GitHub...
echo ===================================================

:: Perintah otomatis update ke GitHub
git add .
git commit -m "Auto-update data anomali %date%"
git push origin main

echo.
echo ===================================================
echo BERHASIL! Web Dashboard sedang diperbarui oleh GitHub.
echo ===================================================
pause
exit

:error
echo.
echo [ERROR] Terjadi kesalahan pada proses di atas. Pipeline dihentikan.
pause
exit