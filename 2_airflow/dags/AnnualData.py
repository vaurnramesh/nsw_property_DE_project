from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import os
import requests
import zipfile
import shutil

DOWNLOAD_DIR = "/opt/airflow/data/"
BASE_URL = "https://www.valuergeneral.nsw.gov.au/__psi/yearly/{year}.zip"

START_YEAR = 2024
END_YEAR = 2024

def extract_inner_zips(zip_path: str, extract_to_dir: str) -> None:
    os.makedirs(extract_to_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_to_dir)

    for root, _, files in os.walk(extract_to_dir):
        for file in files:
            if file.lower().endswith('.zip'):
                inner_zip_path = os.path.join(root, file)
                inner_extract_dir = os.path.splitext(inner_zip_path)[0]
                extract_inner_zips(inner_zip_path, inner_extract_dir)
                os.remove(inner_zip_path)



def collect_dat_files(src_dir: str, dest_dir: str) -> int:
    os.makedirs(dest_dir, exist_ok=True)
    count = 0
    for root, _, files in os.walk(src_dir):
        for file in files:
            if file.lower().endswith('.dat'):
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_dir, file)
                shutil.copy2(src_file, dest_file)
                count += 1
    return count


# -----------------
# Airflow tasks
# -----------------

@task # 1
def download_yearly_zip(year: int) -> str:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    local_path = os.path.join(DOWNLOAD_DIR, f"{year}.zip")

    if not os.path.exists(local_path):
        url = BASE_URL.format(year=year)
        response = requests.get(url)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)
    return local_path


@task # 2
def extract_year_zip(zip_path: str, year: int) -> str:
    extract_to_dir = os.path.join(DOWNLOAD_DIR, str(year))
    extract_inner_zips(zip_path, extract_to_dir)
    return extract_to_dir


@task # 3
def collect_yearly_dat_files(extract_dir: str, year: int) -> str:
    dat_collection_dir = os.path.join(DOWNLOAD_DIR, f"{year}_DATS")
    count = collect_dat_files(extract_dir, dat_collection_dir)
    print(f"Collected {count} .DAT files for {year}")
    return dat_collection_dir


@task # 4
def cleanup_year_files(year: int):

    zip_path = os.path.join(DOWNLOAD_DIR, f"{year}.zip")
    extract_dir = os.path.join(DOWNLOAD_DIR, str(year))

    if os.path.exists(zip_path):
        os.remove(zip_path)
        print(f"Deleted {zip_path}")
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
        print(f"Deleted {extract_dir}")

    return f"Cleaned up files for {year}"



# -----------------
# DAG definition
# -----------------
with DAG(
    dag_id="Annual_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, # Trigger manually
    catchup=False
) as dag:
    
    for year in range(START_YEAR, END_YEAR + 1):
        zip_path=download_yearly_zip(year)
        extracted_dir=extract_year_zip(zip_path, year)
        dat_dir = collect_yearly_dat_files(extracted_dir, year)
        cleanup = cleanup_year_files(year)

        zip_path >> extracted_dir >> dat_dir >> cleanup