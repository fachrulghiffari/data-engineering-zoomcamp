locals {
    data_lake_bucket = "dtc_data_lake"
}

variable "google_credentials_dtc_de" {
    description = "Your credentials file"
  
}
variable "project" {
    description = "Your GCP Project ID"
  
}

variable "region" {
    description = "Region for GCP Resources"
    default = "asia-southeast2"
    type = string
}

variable "storage_class" {
    description = "Storage class type for your bucket. Check official docs for more info."
    default = "STANDARD" 
}

variable "BQ_DATASET" {
    description = "BigQuery Dataset that raw data (from GCS) will be written to"
    type = string
    default = "trips_data_all"
}