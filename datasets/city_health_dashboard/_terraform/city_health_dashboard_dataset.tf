/**
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


resource "google_bigquery_dataset" "city_health_dashboard" {
  dataset_id  = "city_health_dashboard"
  project     = var.project_id
  description = "City Health Dashboard"
}

output "bigquery_dataset-city_health_dashboard-dataset_id" {
  value = google_bigquery_dataset.city_health_dashboard.dataset_id
}

resource "google_storage_bucket" "city-health-dashboard" {
  name                        = "${var.bucket_name_prefix}-city-health-dashboard"
  force_destroy               = true
  location                    = "US"
  uniform_bucket_level_access = true
}

output "storage_bucket-city-health-dashboard-name" {
  value = google_storage_bucket.city-health-dashboard.name
}
