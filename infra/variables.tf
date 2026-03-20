variable "project_id" {
  description = "GCP Project ID"
  type = string
}

variable "region" {
  description = "Project Region"
  type = string
}

variable "credentials_file" {
  description = "Path to GCP service account JSON key file"
  type = string
}

variable "bucket_name" {
  description = "GCS bucket name"
  type = string
} 

variable "bq_dataset_id" {
  description = "BigQuery Dataset ID"
}

variable "service_account_email" {
  description = "GCP Service account email"
  type = string
}

