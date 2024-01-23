variable "gcp_location" {
  description = "The location of the GCP resources to create"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset to create"
  default     = "terraform_dataset"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket to create"
  default     = "terraform_bucket"
}

variable "gcs_storage_class" {
  description = "The storage class of the GCS bucket to create"
  default     = "STANDARD"
}
