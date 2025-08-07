variable "credentials" {
  description = "My Credentials"
  default     = "~/.gc/google_credentials_nsw_prop.json"
}


variable "project" {
  description = "Project"
  default     = "nsw-property-de-project"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "australia-southeast1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "australia-southeast1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "nsw_prop_data_all"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "nsw-prop-de-proj-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}