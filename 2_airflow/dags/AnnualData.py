from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.bash import BashOperator
import pandas as pd

from datetime import datetime
import os
import requests
import zipfile
import shutil

DOWNLOAD_DIR = "/opt/airflow/data/"
BASE_URL = "https://www.valuergeneral.nsw.gov.au/__psi/yearly/{year}.zip"
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GCP_CONN_ID = os.environ.get("AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT")

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
            
            
            raw_postcode = parts[10].strip()
            postcode = raw_postcode if raw_postcode.isdigit() else None

            sale = {
                "lga_code": parts[1].strip() or None,
                "property_id": parts[2].strip(), # store as string
                "processed_datetime": pd.to_datetime(parts[4], format="%Y%m%d %H:%M", errors="coerce"),
                "building_name": parts[5].strip() or None,
                "section_no": parts[6].strip() or None,
                "street_no": parts[7].strip() or None,
                "street_name": parts[8].strip() or None,
                "locality": parts[9].strip() or None,
                "postcode": postcode,
                "land_area_sqm": pd.to_numeric(parts[11], errors="coerce"),
                "area_type": parts[12].strip() or None,
                "contract_date": pd.to_datetime(parts[13], format="%Y%m%d", errors="coerce"),
                "settlement_date": pd.to_datetime(parts[14], format="%Y%m%d", errors="coerce"),
                "sale_price": pd.to_numeric(parts[15], errors="coerce"),
                "zoning": parts[16].strip() or None,
                "property_category": parts[17].strip() or None,
                "property_description": property_description,
            }
            sales.append(sale)

    df = pd.DataFrame(sales)

    # Now apply nullable integer conversion after DataFrame creation
    int_cols = ["land_area_sqm", "sale_price"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")         

    return df


def upload_to_gcs(bucket_name: str, object_name: str, local_file: str, gcp_conn_id: str):
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    hook.upload(bucket_name=bucket_name, object_name=object_name, filename=local_file)


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
def process_dat_files(dat_dir: str, year: int) -> str:
    all_sales = []
    dat_files = [f for f in os.listdir(dat_dir) if f.lower().endswith(".dat")]

    for i, filename in enumerate(dat_files, 1):
        file_path = os.path.join(dat_dir, filename)
        try:
            df = parse_valnet_dat(file_path)
            if df.empty:
                print(f"Warning: No valid data found in {filename}")
            else:
                all_sales.append(df)
        except Exception as e:
            print(f"Error processing {filename}: {e}")

        print(f"Processing DAT files: {i}/{len(dat_files)}", end="\r")

    if all_sales:
        final_df = pd.concat(all_sales, ignore_index=True)
        parquet_path = os.path.join(DOWNLOAD_DIR, f"all_sales_{year}.parquet")
        final_df.to_parquet(parquet_path, index=False, engine="pyarrow")
        print(f"\n  -> Saved {len(final_df)} records to {parquet_path}")
        return parquet_path
    else:
        print("\nNo dat files found or all files failed to parse.")
        return "" 

@task # 5
def upload_year_to_gcs(parquet_path: str, year: int, bucket: str, gcp_conn_id: str):
    if parquet_path and os.path.exists(parquet_path):
        object_name = f"raw/yearly/all_sales_{year}.parquet"
        upload_to_gcs(bucket_name=bucket, object_name=object_name, local_file=parquet_path, gcp_conn_id=gcp_conn_id)
        print(f"Uploaded {parquet_path} to gs://{bucket}/{object_name}")
    else:
        print(f"Skipping upload for {year} — file not found")


# -----------------
# DAG definition
# -----------------
with DAG(
    dag_id="Annual_data",
    start_date=datetime(START_YEAR, 1, 1),
    schedule_interval=None, # Trigger manually
    catchup=False,
    max_active_tasks=8
) as dag:
    
    for year in range(START_YEAR, END_YEAR + 1):
        with TaskGroup(group_id=f"year_{year}") as tg:
            zip_task = download_yearly_zip.override(task_id=f"download_{year}")(year)
            extract_task = extract_year_zip.override(task_id=f"extract_{year}")(zip_task, year)
            dat_task = collect_yearly_dat_files.override(task_id=f"collect_{year}")(extract_task, year)
            process_task = process_dat_files.override(task_id=f"process_{year}")(dat_task, year)
            upload_task = upload_year_to_gcs.override(task_id=f"upload_{year}")(
                process_task, year, BUCKET, GCP_CONN_ID
            )
            cleanup_task = BashOperator(
            task_id=f"cleanup_{year}",
            bash_command=f"""
                rm -f {DOWNLOAD_DIR}{year}.zip
                rm -rf {DOWNLOAD_DIR}{year}
                rm -rf {DOWNLOAD_DIR}{year}_DATS
                rm -f {DOWNLOAD_DIR}all_sales_{year}.parquet
            """
        )

        zip_task >> extract_task >> dat_task >> process_task >> upload_task >> cleanup_task