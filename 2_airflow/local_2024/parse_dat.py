import os
import pandas as pd

SRC_DIR = "../local_2024/data_test/2024_DATS"
DEST_DIR = "../local_2024/final_output/"
os.makedirs(DEST_DIR, exist_ok=True)

def parse_valnet_dat(file_path):
    sales = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(";")
            if parts[0] != "B":
                continue

            property_description = parts[18].strip().upper()
            # Only keep Residence or Vacant Land
            if property_description not in {"RESIDENCE", "VACANT LAND"}:
                continue

            sale = {
                "lga_code": parts[1],
                "property_id": pd.to_numeric(parts[2], errors="coerce"),
                "processed_datetime": pd.to_datetime(parts[4], format="%Y%m%d %H:%M", errors="coerce"),
                "building_name": parts[5] or None,
                "section_no": parts[6],
                "street_no": parts[7],
                "street_name": parts[8],
                "locality": parts[9],
                "postcode": parts[10],
                "land_area_sqm": pd.to_numeric(parts[11], errors="coerce"),
                "area_type": parts[12],
                "contract_date": pd.to_datetime(parts[13], format="%Y%m%d", errors="coerce"),
                "settlement_date": pd.to_datetime(parts[14], format="%Y%m%d", errors="coerce"),
                "sale_price": pd.to_numeric(parts[15], errors="coerce"),
                "zoning": parts[16],
                "property_category": parts[17],
                "property_description": property_description,
            }
            sales.append(sale)

    return pd.DataFrame(sales)


all_sales = []
dat_files = [f for f in os.listdir(SRC_DIR) if f.lower().endswith(".dat")]

for i, filename in enumerate(dat_files, 1):
    file_path = os.path.join(SRC_DIR, filename)
    try:
        df = parse_valnet_dat(file_path)
        if df.empty:
            print(f"Warning: No valid data found in {filename}")
        else:
            all_sales.append(df)
    except Exception as e:
        print(f"Error processing {filename}: {e}")

    print(f"Processing DAT files: {i}/{len(dat_files)}", end="\r") # \r doesn't move the cursor and ends up printing on the same line 

if all_sales:
    final_df = pd.concat(all_sales, ignore_index=True)
    csv_path = os.path.join(DEST_DIR, "all_sales_2024.csv")
    final_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n  -> Saved {len(final_df)} records to {csv_path}")
else:
    print("\nNo dat files found or all files failed to parse.")
