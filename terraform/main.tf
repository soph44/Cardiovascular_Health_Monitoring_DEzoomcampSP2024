terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "cvd-bucket-de2024" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

data "google_service_account" "default" {
  project      = var.project
  account_id   = var.service_account_id
}

resource "google_storage_bucket_object" "main_python_file" {
  name   = "etl/full_workflow_gcs_bq_etl.py"
  source = "../etl/full_workflow_gcs_bq_etl.py"
  bucket = var.gcs_bucket_name
  depends_on = [ google_storage_bucket.cvd-bucket-de2024 ]
}

resource "google_storage_bucket_object" "cluster_init_file" {
  name   = "etl/init_dataproc_modules.sh"
  source = "../lib/init_dataproc_modules.sh"
  bucket = var.gcs_bucket_name
  depends_on = [ google_storage_bucket.cvd-bucket-de2024 ]
}

resource "google_bigquery_dataset" "cvd-sp-de-zoomcamp-2024" {
  dataset_id = var.bq_dataset_name
  location   = var.location
  delete_contents_on_destroy = true
}

resource "google_dataproc_cluster" "cvd_dataproc_cluster" {
  name   = var.dp_cluster_name
  region = var.dp_region
  project = var.project
  depends_on = [ data.google_service_account.default, google_storage_bucket_object.main_python_file ]

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n2-standard-4"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb =50
      }
    }

    software_config {
      override_properties = {
        "dataproc:dataproc.allow.zero.workers": "true"
      }
    }
  
    gce_cluster_config {
      service_account = data.google_service_account.default.email
      service_account_scopes = ["cloud-platform"]
    }
  
    # You can define multiple initialization_action blocks
    initialization_action {
      script      = "gs://cvd-bucket-de2024/etl/init_dataproc_modules.sh"
      timeout_sec = 500
    }
  }
}

resource "google_dataproc_job" "pyspark" {
  region       = var.dp_region
  force_delete = true
  depends_on = [ google_dataproc_cluster.cvd_dataproc_cluster ]
  placement {
    cluster_name = var.dp_cluster_name
  }

  pyspark_config {
    main_python_file_uri = var.main_python_file_uri
    jar_file_uris = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.37.0.jar"]
    args = ["--year=2019"]
    properties = {
      "spark.logConf" = "true"
    }
  }
}




resource "google_dataproc_workflow_template" "template" {
  name = var.wf_template_name
  location = var.region
  #depends_on = [ data.google_service_account.default, google_storage_bucket_object.main_python_file ]

  placement {
    managed_cluster {
      cluster_name = var.wf_cluster_name
      config {
          master_config {
            num_instances = 1
            machine_type  = "n2-standard-4"
            disk_config {
              boot_disk_type    = "pd-ssd"
              boot_disk_size_gb = 50
            }
          }
          gce_cluster_config {
            service_account = data.google_service_account.default.email
            service_account_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
          }
          initialization_actions {
              executable_file = "gs://cvd-bucket-de2024/etl/init_dataproc_modules.sh"
          }
        }
      }
    }
  jobs {
    step_id = "cvd-scheduled-job"
    pyspark_job {
      main_python_file_uri = var.main_python_file_uri
      jar_file_uris = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.37.0.jar"]
      properties = {
        "spark.logConf" = "true"
    }
    }
  }
}
resource "google_dataproc_workflow_template" "template-2020" {
  name = "2020-data-filler"
  location = var.region
  #depends_on = [ data.google_service_account.default, google_storage_bucket_object.main_python_file ]

  placement {
    managed_cluster {
      cluster_name = var.wf_cluster_name
      config {
          master_config {
            num_instances = 1
            machine_type  = "n2-standard-4"
            disk_config {
              boot_disk_type    = "pd-ssd"
              boot_disk_size_gb = 50
            }
          }
          gce_cluster_config {
            service_account = data.google_service_account.default.email
            service_account_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
          }
          initialization_actions {
              executable_file = "gs://cvd-bucket-de2024/etl/init_dataproc_modules.sh"
          }
        }
      }
    }
  jobs {
    step_id = "cvd-scheduled-job"
    pyspark_job {
      main_python_file_uri = var.main_python_file_uri
      jar_file_uris = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.37.0.jar"]
      args = ["--year=2020"]
      properties = {
        "spark.logConf" = "true"
    }
    }
  }
}
resource "google_dataproc_workflow_template" "template-2021" {
  name = "2021-data-filler"
  location = var.region
  #depends_on = [ data.google_service_account.default, google_storage_bucket_object.main_python_file ]

  placement {
    managed_cluster {
      cluster_name = var.wf_cluster_name
      config {
          master_config {
            num_instances = 1
            machine_type  = "n2-standard-4"
            disk_config {
              boot_disk_type    = "pd-ssd"
              boot_disk_size_gb = 50
            }
          }
          gce_cluster_config {
            service_account = data.google_service_account.default.email
            service_account_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
          }
          initialization_actions {
              executable_file = "gs://cvd-bucket-de2024/etl/init_dataproc_modules.sh"
          }
        }
      }
    }
  jobs {
    step_id = "cvd-etl-job"
    pyspark_job {
      main_python_file_uri = var.main_python_file_uri
      jar_file_uris = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.37.0.jar"]
      args = ["--year=2021"]
      properties = {
        "spark.logConf" = "true"
    }
    }
  }
}

resource "google_cloud_scheduler_job" "job" {
  name             = "cdc-pull-schedule"
  description      = "test http job"
  schedule         = "0 12 1 1 *"
  time_zone        = "America/Los_Angeles"

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "POST"
    uri         = "https://dataproc.googleapis.com/v1/projects/${var.project}/regions/${var.region}/workflowTemplates/${var.wf_template_name}:instantiate?alt=json"
    oauth_token {
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
      service_account_email = data.google_service_account.default.email
    }
  }

}
