# Terraform GCP Storage Bucket and BigQuery Dataset Setup

This Terraform configuration provisions the following resources in the Google Cloud project `nsw-property-de-project`:

- A **Google Cloud Storage bucket** with lifecycle rules
- A **BigQuery dataset** with a valid dataset ID

---

## Prerequisites

- Google Cloud SDK installed and configured
- A GCP service account JSON key with appropriate permissions:
  - 
  - `roles/storage.admin`
  - `roles/bigquery.admin`
- Terraform installed (version supporting `hashicorp/google` provider)

---

## Setup and Usage

1. Place your service account credentials

   Save your service account JSON key to a local directory, e.g.,

   ```bash
   ~/.gc/google_credentials_nsw_prop.json
   ```

2. Update Terraform variables

    Make sure the `variables.tf` or your variable definitions reflect


3. Initialize Terraform

    `terraform init`

4. Preview changes

    `terraform plan`

5. Apply Changes

    `terraform apply`

This will create:

The Cloud Storage bucket `nsw-prop-de-proj-terra-bucket` with a lifecycle rule to abort incomplete multipart uploads after 1 day

The BigQuery dataset `nsw_prop_data_all` in the specified location


## Notes

- Ensure your **GCS bucket name** is globally unique and follows [Google Cloud Storage naming conventions](https://cloud.google.com/storage/docs/naming):
  - Must be between 3 and 63 characters.
  - Lowercase letters, numbers, dashes (`-`).
  - **No underscores** or uppercase letters.

- BigQuery **dataset IDs** must:
  - Contain only **letters**, **numbers**, and **underscores**.
  - Start with a letter or underscore.
  - Be at most 1,024 characters.

- **Do not commit** your credentials JSON file (e.g., service account key) to version control:
  - Add it to `.gitignore`
  - Example:
    ```
    *.json
    credentials/
    ```

- Follow **least privilege principles** when assigning roles to your service account:
  - Grant only the permissions needed.
  - Avoid assigning broad roles like `roles/editor` unless absolutely necessary.


## Clean up

To remove the resources when no longer needed, run:

`terraform destroy`