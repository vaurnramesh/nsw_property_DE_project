# Airflow

## Download yearly ZIP from NSW property website

This DAG automates the retrieval of property sales data from the official NSW Valuer General site.

* **Source**: The data is sourced from the *Property Sales Information* dataset: https://valuation.property.nsw.gov.au/embed/propertySalesInformation

* **Process**:
   1. For each year in the specified range (`START_YEAR` to `END_YEAR`), the script constructs a download URL.
   2. The yearly ZIP file is fetched via an HTTP GET request using Python's `requests` library.
   3. Downloaded files are stored in the Airflow worker's data directory, which is mounted to the host machine for persistence.

* **Mounted Volume for Local Access**: A Docker volume is mapped to allow access to files directly from your host machine:

```yaml
volumes:
  - ./data:/opt/airflow/data
```

This means all downloaded `.zip` and processed `.DAT` files will appear in your local `./data` folder.

### Extract Inner Zips

The yearly ZIP contains multiple **nested ZIP files** that must be extracted before accessing the `.DAT` files.

* **Logic**:
   1. The top-level ZIP is extracted into a `year/` folder inside `/opt/airflow/data`.
   2. The script recursively scans for any `.zip` files inside the extracted structure.
   3. Each inner ZIP is extracted into a folder named after its base file name.
   4. After extraction, the original inner ZIP files are deleted to save space.

* **Why Recursive Extraction Matters**: Without recursion, the nested archives would remain zipped, and the `.DAT` files inside would be inaccessible for the later ingestion step.

* **Example**:

```
2024.zip
└── 2024/
    ├── sales_jan.zip → extracted to sales_jan/
    ├── sales_feb.zip → extracted to sales_feb/
    └── ...
```

### Collect DAT Files

The final step consolidates all `.DAT` files from the nested directory structure into a single organized location.

* **Process**:
   1. The script recursively walks through all extracted directories for each year.
   2. All files with the `.dat` extension are identified and collected.
   3. These files are copied to a centralized `{year}_DATS/` directory for easy access.
   4. The total count of collected files is logged for verification.

* **Output Structure**:

```
./data/
├── 2024.zip                    # Original downloaded ZIP
├── 2024/                       # Extracted nested structure
│   ├── sales_jan/
│   │   ├── file1.dat
│   │   └── file2.dat
│   └── sales_feb/
│       ├── file3.dat
│       └── file4.dat
└── 2024_DATS/                  # Consolidated DAT files
    ├── file1.dat
    ├── file2.dat
    ├── file3.dat
    └── file4.dat
```

### Parse/ Transform `.DAT` files to CSV / Parquet


## Upload Parquets to GCS


## Create BigQuery datasets and tables


## Run Deduplication/merging queries in BigQuery to consolidate data


## Cleanup local temp files


## SETUP

### Virtual Environment

```bash
echo 'export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.gc/google_credentials_nsw_prop.json"' >> ~/.zshrc

source ~/.zshrc

echo $GOOGLE_APPLICATION_CREDENTIALS
```

You should now see your new google credentials being activated.