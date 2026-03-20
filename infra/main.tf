terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.24.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

resource "google_storage_bucket" "bronze" {
    name          = var.bucket_name
    location      = var.region
    force_destroy = true #before deleting a bucket, delete all objects within the bucket, or Anywhere Caches caching data for that bucket.
    versioning {
    enabled = true
  }
  lifecycle_rule {
    condition {
      age = 90 #if bronze objects older than 90 days -> deleting
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "main" {
  dataset_id  = var.bq_dataset_id
  location    = var.region
  description = "BTS Airline On-Time Performance dataset"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${var.service_account_email}"
}

resource "google_project_iam_member" "bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${var.service_account_email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.service_account_email}"
}