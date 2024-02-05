terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.12.0"
    }
  }
}

provider "google" {
  # Configuration options
  project     = "de-course-411516"
  region      = "us-central1"
  credentials = "./keys/de-course-411516-c64d64bd809f.json"
}

resource "google_storage_bucket" "demo_bucket" {
  name          = var.gcs_bucket_name
  location      = var.gcp_location
  storage_class = var.gcs_storage_class
  force_destroy = true
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.gcp_location
  delete_contents_on_destroy = true
}
