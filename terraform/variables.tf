variable "credentials" {
  description = "Google Cloud credentials"
  # CHANGE to location of google cloud service account credentials
  default     = "../keys/cvd-key.json"
}

variable "service_account_id" {
  description = "Service account id (the one you created for the terraform part of the zoomcamp)"
  # CHANGE to service account id
  default     = "cvd-service"
} 

variable "project" {
  description = "Project"
  # CHANGE to your project ID. Remeber to change in the Python script too.
  default     = "cvd-sp-de-zoomcamp-2024"
}

variable "location" {
  description = "Project Location"
  # Update if desired. Or leave as is.
  default     = "US"
}

variable "region" {
  description = "Region"
  #Update as desired or leave as is. Please select a region that supports Cloud Scheduler.
  default     = "us-west1"
}

variable "dp_region" {
  description = "Update or leave as is. Must match project region above. europe-west10-a is not compatible."
  default     = "us-west1"
}




variable "gcs_bucket_name" {
  description = "Unique bucket name"
  # ONLY CHANGE IF BUCKET ACCESS ERROR OCCURS
  default     = "cvd-bucket-de2024"
}




# DO NOT CHANGE ANY BELOW
variable "bq_dataset_name" {
  description = "Dataset Name"
  default     = "cvd_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "dp_cluster_name" {
  description = "Dataproc cluster name"
  default = "dp-cvd-cluster"
}

variable "main_python_file_uri" {
  description = "URL to main mython file"
  default = "gs://cvd-bucket-de2024/etl/full_workflow_gcs_bq_etl.py"
}

variable "wf_template_name" {
  description = "Workflow Template name"
  default = "cvd_wf_template"
}

variable "wf_cluster_name" {
  description = "Workflow cluster name"
  default = "cvd-wf-cluster"
}


