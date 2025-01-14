# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from airflow import DAG
from airflow.contrib.operators import gcs_to_bq, kubernetes_pod_operator

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="cfpb_complaints.complaint_database",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    complaint_database_transform_csv = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="complaint_database_transform_csv",
        startup_timeout_seconds=600,
        name="complaint_database",
        namespace="default",
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": ["pool-e2-standard-4"],
                                }
                            ]
                        }
                    ]
                }
            }
        },
        image_pull_policy="Always",
        image="{{ var.json.cfpb_complaints.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URL": "http://files.consumerfinance.gov/ccdb/complaints.csv.zip",
            "SOURCE_FILE": "files/data.zip",
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/cfpb_complaints/complaint_database/data_output.csv",
            "PIPELINE_NAME": "complaint_database",
            "CSV_HEADERS": '["date_received","product","subproduct","issue","subissue","consumer_complaint_narrative","company_public_response","company_name","state","zip_code","tags","consumer_consent_provided","submitted_via","date_sent_to_company","company_response_to_consumer","timely_response","consumer_disputed","complaint_id"]',
            "RENAME_MAPPINGS": '{"Complaint ID":"complaint_id","Date received":"date_received","Product":"product","Sub-product":"subproduct","Issue":"issue","Sub-issue":"subissue","Consumer complaint narrative":"consumer_complaint_narrative","Company response to consumer":"company_response_to_consumer","Company public response":"company_public_response","Company":"company_name","State":"state","ZIP code":"zip_code","Tags":"tags","Consumer consent provided?":"consumer_consent_provided","Submitted via":"submitted_via","Date sent to company":"date_sent_to_company","Timely response?":"timely_response","Consumer disputed?":"consumer_disputed"}',
        },
        resources={"request_memory": "3G", "request_cpu": "1"},
    )

    # Task to load CSV data to a BigQuery table
    load_complaint_database_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_complaint_database_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/cfpb_complaints/complaint_database/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="cfpb_complaints.complaint_database",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "date_received",
                "type": "date",
                "description": "Date the complaint was received by the CPFB",
                "mode": "nullable",
            },
            {
                "name": "product",
                "type": "string",
                "description": "The type of product the consumer identified in the complaint",
                "mode": "nullable",
            },
            {
                "name": "subproduct",
                "type": "string",
                "description": "The type of sub-product the consumer identified in the complaint",
                "mode": "nullable",
            },
            {
                "name": "issue",
                "type": "string",
                "description": "The issue the consumer identified in the complaint",
                "mode": "nullable",
            },
            {
                "name": "subissue",
                "type": "string",
                "description": "The sub-issue the consumer identified in the complaint",
                "mode": "nullable",
            },
            {
                "name": "consumer_complaint_narrative",
                "type": "string",
                "description": "A description of the complaint provided by the consumer",
                "mode": "nullable",
            },
            {
                "name": "company_public_response",
                "type": "string",
                "description": "The company's optional public-facing response to a consumer's complaint",
                "mode": "nullable",
            },
            {
                "name": "company_name",
                "type": "string",
                "description": "Name of the company identified in the complaint by the consumer",
                "mode": "nullable",
            },
            {
                "name": "state",
                "type": "string",
                "description": "Two letter postal abbreviation of the state of the mailing address provided by the consumer",
                "mode": "nullable",
            },
            {
                "name": "zip_code",
                "type": "string",
                "description": "The mailing ZIP code provided by the consumer",
                "mode": "nullable",
            },
            {
                "name": "tags",
                "type": "string",
                "description": "Data that supports easier searching and sorting of complaints",
                "mode": "nullable",
            },
            {
                "name": "consumer_consent_provided",
                "type": "string",
                "description": "Identifies whether the consumer opted in to publish their complaint narrative",
                "mode": "nullable",
            },
            {
                "name": "submitted_via",
                "type": "string",
                "description": "How the complaint was submitted to the CFPB",
                "mode": "nullable",
            },
            {
                "name": "date_sent_to_company",
                "type": "date",
                "description": "The date the CFPB sent the complaint to the company",
                "mode": "nullable",
            },
            {
                "name": "company_response_to_consumer",
                "type": "string",
                "description": "The response from the company about this complaint",
                "mode": "nullable",
            },
            {
                "name": "timely_response",
                "type": "boolean",
                "description": "Indicates whether the company gave a timely response or not",
                "mode": "nullable",
            },
            {
                "name": "consumer_disputed",
                "type": "boolean",
                "description": "Whether the consumer disputed the company's response",
                "mode": "nullable",
            },
            {
                "name": "complaint_id",
                "type": "string",
                "description": "Unique ID for complaints registered with the CFPB",
                "mode": "nullable",
            },
        ],
    )

    complaint_database_transform_csv >> load_complaint_database_to_bq
