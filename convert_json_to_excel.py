import json
import pandas as pd
import os
import re
import glob
import shutil
import tempfile
import sys

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

INPUT_DIR = r"J:\My Drive\Colab Notebooks\input\06032026"
OUTPUT_DIR = os.path.join(INPUT_DIR, "output")
LOCAL_TEMP = os.path.join(tempfile.gettempdir(), "json_convert_temp")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOCAL_TEMP, exist_ok=True)

def extract_kbli(kegiatan_usaha):
    """Extract KBLI code from kegiatan_usaha string."""
    if pd.isna(kegiatan_usaha) or not kegiatan_usaha:
        return None
    match = re.search(r'KBLI:\s*(\S+)', str(kegiatan_usaha))
    if match:
        value = match.group(1).rstrip(']').rstrip(',')
        return value if value and value != '-' else None
    return None

# Collect all JSON files
json_files = glob.glob(os.path.join(INPUT_DIR, "*.json"))
print(f"Found {len(json_files)} JSON files")

# Read all JSON files - copy to local first for speed
all_dfs = []
for jf in json_files:
    basename = os.path.basename(jf)
    local_path = os.path.join(LOCAL_TEMP, basename)
    
    # Copy to local temp if not already there
    if not os.path.exists(local_path):
        print(f"\nCopying {basename} to local temp...")
        shutil.copy2(jf, local_path)
        print(f"  Copied.")
    
    print(f"Reading {basename} from local temp...")
    with open(local_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    all_dfs.append(df)
    print(f"  -> {len(df)} rows loaded")
    del data
    
    # Remove local copy to save space
    os.remove(local_path)

df_all = pd.concat(all_dfs, ignore_index=True)
del all_dfs
print(f"\nTotal rows: {len(df_all)}")

# Extract KBLI column
print("Extracting KBLI...")
df_all['KBLI'] = df_all['kegiatan_usaha'].apply(extract_kbli)
print(f"KBLI extracted. Non-null KBLI count: {df_all['KBLI'].notna().sum()}")

# Split by kdkab and save to separate Excel files
kdkab_values = df_all['kdkab'].unique()
print(f"\nFound {len(kdkab_values)} unique kdkab values")
print("Saving Excel files...")

for kdkab in sorted(kdkab_values):
    df_kab = df_all[df_all['kdkab'] == kdkab].copy()
    
    nmkab_values = df_kab['nmkab'].dropna().unique()
    nmkab = nmkab_values[0] if len(nmkab_values) > 0 else "UNKNOWN"
    nmkab_clean = re.sub(r'[^\w\s-]', '', nmkab).strip()
    
    output_file = os.path.join(OUTPUT_DIR, f"SBR_{kdkab}_{nmkab_clean}.xlsx")
    df_kab.to_excel(output_file, index=False, engine='openpyxl')
    print(f"  Saved: SBR_{kdkab}_{nmkab_clean}.xlsx ({len(df_kab)} rows)")

# Cleanup temp dir
shutil.rmtree(LOCAL_TEMP, ignore_errors=True)

print("\nDone! All files saved to:", OUTPUT_DIR)
