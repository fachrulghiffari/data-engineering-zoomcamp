terraform{
    required_version = "1.5.4"
    backend "local" {}
    required_providers {
      google = {
        source = "hashicorp/google"
      }
    }
}

provider "google" {
    project = var.project
    region = var.region
    credentials = var.google_credentials_dtc_de
}

resource "google_storage_bucket" "data_lake_bucket" {
    name = "${local.data_lake_bucket}_${var.project}"
    location = var.region

    storage_class = var.storage_class
    uniform_bucket_level_access = true
    
    versioning{
        enabled = true
    }

    lifecycle_rule {
      action {
        type = "Delete"
      }
      condition {
        age = 30 //days
      }
    }

    force_destroy =  true
}

resource "google_bigquery_dataset" "dataset" {
    dataset_id = var.BQ_DATASET
    project = var.project
    location = var.region
}