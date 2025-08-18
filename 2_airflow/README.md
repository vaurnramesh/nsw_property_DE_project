# Airflow

## Table of Contents
- [Airflow](#airflow)
  - [Table of Contents](#table-of-contents)
  - [SETUP](#setup)
    - [Credentials setup](#credentials-setup)
    - [Add Google Cloud Connection in Airflow UI](#add-google-cloud-connection-in-airflow-ui)
  - [DAG 1: AnnualData](#dag-1-annualdata)
    - [Phase 1 - Extract](#phase-1---extract)
      - [Download yearly ZIP from NSW property website](#download-yearly-zip-from-nsw-property-website)
      - [Extract Inner Zips](#extract-inner-zips)
    - [Phase 2 - Transform](#phase-2---transform)
      - [Collect DAT Files](#collect-dat-files)
      - [Parse/ Transform `.DAT` files to CSV / Parquet](#parse-transform-dat-files-to-csv--parquet)
    - [Phase 3: Load](#phase-3-load)
      - [Upload Annual data Parquets to GCS](#upload-annual-data-parquets-to-gcs)
      - [Create BigQuery datasets and tables](#create-bigquery-datasets-and-tables)
      - [Run Deduplication/merging queries in BigQuery to consolidate data](#run-deduplicationmerging-queries-in-bigquery-to-consolidate-data)
      - [Cleanup local temp files](#cleanup-local-temp-files)


## SETUP

### Credentials setup

```bash
echo 'export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.gc/google_credentials_nsw_prop.json"' >> ~/.zshrc

source ~/.zshrc

echo $GOOGLE_APPLICATION_CREDENTIALS

# To activate the google creds

gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS 
```

You should now see your new google credentials being activated.

### Add Google Cloud Connection in Airflow UI

To allow Airflow to interact with Google Cloud Storage, you need to add a connection using your service account credentials.

1. **Navigate to the Airflow UI:**  
   `Admin → Connections → + (Add a new record)`

2. **Fill in the connection details:**

   | Field             | Value / Instructions                                         |
   |------------------|---------------------------------------------------------------|
   | **Connection Id** | Any sensible name or your Google Cloud Project ID            |
   | **Connection Type** | `Google Cloud`                                              |
   | **Project Id**     | Your Google Cloud Project ID                                 |
   | **Keyfile JSON**   | Paste the contents of your service account JSON file located at `$GOOGLE_APPLICATION_CREDENTIALS` |

3. **Save** the connection.

> Using the Keyfile JSON directly avoids the need to mount local credentials into your Docker container or store secrets in source code.

## DAG 1: AnnualData

### Phase 1 - Extract

#### Download yearly ZIP from NSW property website

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

#### Extract Inner Zips

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

### Phase 2 - Transform

#### Collect DAT Files

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

#### Parse/ Transform `.DAT` files to CSV / Parquet

Once all `.DAT` files are collected, the next step is to parse and normalize them into structured tabular formats suitable for downstream analytics.

**Process**
1. Each .DAT file is read using a predefined schema.

2. Raw fields (including dates and times) are cleaned and standardized. All date/time fields are converted to Unix timestamps to avoid multiple datetime formats and simplify downstream usage.

3. Transformed datasets are written to both CSV and Parquet formats for compatibility and performance: CSV provides human-readable, universal accessibility. While parquet provides efficient columnar storage optimized for big-data workloads.

4. Logs are generated for each transformation run, capturing: File name processed, Row counts before/after transformation and any parsing/conversion errors.


### Phase 3: Load

This phase takes the transformed data and makes it queryable in BigQuery. It consists of four steps: uploading the processed files, preparing datasets, running consolidation queries, and cleaning up temporary files.

#### Upload Annual data Parquets to GCS

* The transformed data (by year) is written out as Parquet files.

* These files are uploaded to Google Cloud Storage (GCS) into a structured bucket path

* Parquet is used as it provides efficient columnar storage, reduces storage cost, and improves query performance in BigQuery external tables.

#### Create BigQuery datasets and tables

* Create or reuse a BigQuery dataset (e.g., project.dataset_name) that will contain all processed tables.

* Define external tables pointing to the uploaded GCS parquet files for raw querying/debugging.

* Create temporary (staging) tables in BigQuery with proper type casting and schema alignment.

* Finally, create the final tables that serve as the authoritative data source for downstream use cases.

#### Run Deduplication/merging queries in BigQuery to consolidate data

* Use SQL scripts to remove duplicate rows caused by source overlaps or re-ingested files.

* Merge annual partitions into a single consolidated table (partitioned by year or event date for performance).

* Ensure schema consistency (e.g., casting to STRING, INTEGER, TIMESTAMP where necessary).

* Validate record counts between external → tmp → final tables to catch data loss or anomalies.

#### Cleanup local temp files

* Remove temporary Parquet files stored locally after successful upload to GCS.

* Clear out any intermediate logs or scratch directories.

* This keeps local disk usage minimal and avoids accidental re-uploads.


