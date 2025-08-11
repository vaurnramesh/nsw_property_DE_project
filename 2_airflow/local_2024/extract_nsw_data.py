import os
import requests
import zipfile
import shutil
from tqdm import tqdm

DOWNLOAD_DIR = "../local_2024/data_test"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

BASE_URL = "https://www.valuergeneral.nsw.gov.au/__psi/yearly/{year}.zip"

# Range of years to download
START_YEAR = 2024
END_YEAR = 2024

def extract_inner_zips(zip_path: str, extract_to_dir: str) -> None:
    """
    Recursively extract all zip files starting from zip_path into extract_to_dir.
    Extracts nested zip files under extract_to_dir as well.
    """
    os.makedirs(extract_to_dir, exist_ok=True)
    print(f"Extracting ZIP: {zip_path} --> {extract_to_dir}")

   
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_to_dir)

    found_inner_zips = False
    for root, _, files in os.walk(extract_to_dir):
        for file in files:
            if file.lower().endswith('.zip'):
                found_inner_zips = True
                inner_zip_path = os.path.join(root, file)
                inner_extract_dir = os.path.splitext(inner_zip_path)[0]
                print(f"Found inner zip: {inner_zip_path}. Extracting recursively into {inner_extract_dir}")
                extract_inner_zips(inner_zip_path, inner_extract_dir)
                os.remove(inner_zip_path)
    if not found_inner_zips:
        print(f"No more inner zips found in {extract_to_dir}")


def collect_dat_files(src_dir: str, dest_dir: str) -> None:
    os.makedirs(dest_dir, exist_ok=True)
    count = 0

    for root, _, files in os.walk(src_dir):
        for file in files:
            if file.lower().endswith('.dat'):
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_dir, file)

                shutil.copy2(src_file, dest_file)
                count += 1

    print(f"Collected {count} .DAT files into {dest_dir}")

for year in range(START_YEAR, END_YEAR + 1):
    url = BASE_URL.format(year=year)
    local_path = os.path.join(DOWNLOAD_DIR, f"{year}.zip")
    
    if os.path.exists(local_path):
        print(f"{year} already downloaded.")
    else:
        print(f"Downloading {year} from {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(response.content)
            print(f"Downloaded {year} successfully")
        except Exception as ex:
            print(f"Failed to download {year}: {ex}")
            continue
    
   
    extract_to_dir = os.path.join(DOWNLOAD_DIR, str(year))
    extract_inner_zips(local_path, extract_to_dir)

    dat_collection_dir = os.path.join(DOWNLOAD_DIR, f"{year}_DATS")
    collect_dat_files(extract_to_dir, dat_collection_dir)
