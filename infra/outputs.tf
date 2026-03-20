output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.bronze.name
}

output "bq_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.main.dataset_id
}