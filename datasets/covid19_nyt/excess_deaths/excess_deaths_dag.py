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
    dag_id="covid19_nyt.excess_deaths",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    excess_deaths__transform_csv = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="excess_deaths__transform_csv",
        startup_timeout_seconds=600,
        name="mobility_report",
        namespace="default",
        affinity={'nodeAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': {'nodeSelectorTerms': [{'matchExpressions': [{'key': 'cloud.google.com/gke-nodepool', 'operator': 'In', 'values': ['pool-e2-standard-4']}]}]}}},
        image_pull_policy="Always",
        image="{{ var.json.covid19_nyt.container_registry.run_csv_transform_kub }}",
        env_vars={'SOURCE_URL': 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/excess-deaths/deaths.csv', 'SOURCE_FILE': 'files/data.csv', 'TARGET_FILE': 'files/data_output.csv', 'TARGET_GCS_BUCKET': '{{ var.value.composer_bucket }}', 'TARGET_GCS_PATH': 'data/covid19_nyt/excess_deaths/data_output.csv', 'PIPELINE_NAME': 'excess_deaths', 'CSV_HEADERS': '["country" ,"placename" ,"frequency" ,"start_date" ,"end_date" ,"year" ,"month" ,"week" ,"deaths" ,"expected_deaths" ,"excess_deaths" ,"baseline"]', 'RENAME_MAPPINGS': '{"country":"country" ,"placename":"placename" ,"frequency":"frequency" ,"start_date":"start_date" ,"end_date":"end_date" ,"year":"year" ,"month":"month" ,"week":"week" ,"deaths":"deaths" ,"expected_deaths":"expected_deaths" ,"excess_deaths":"excess_deaths" ,"baseline":"baseline"}'},
        resources={'limit_memory': '2G', 'limit_cpu': '1'},
    )

    # Task to load CSV data to a BigQuery table
    load_covid19_nyt_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_covid19_nyt_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=['data/covid19_nyt/excess_deaths/data_output.csv'],
        source_format="CSV",
        destination_project_dataset_table="covid19_nyt/excess_deaths",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[{'name': 'country', 'type': 'STRING', 'mode': 'nullable'}, {'name': 'placename', 'type': 'STRING', 'mode': 'nullable'}, {'name': 'frequency', 'type': 'STRING', 'mode': 'nullable'}, {'name': 'start_date', 'type': 'DATE', 'mode': 'nullable'}, {'name': 'end_date', 'type': 'DATE', 'mode': 'nullable'}, {'name': 'year', 'type': 'STRING', 'mode': 'nullable'}, {'name': 'month', 'type': 'INTEGER', 'mode': 'nullable'}, {'name': 'week', 'type': 'INTEGER', 'mode': 'nullable'}, {'name': 'deaths', 'type': 'INTEGER', 'mode': 'nullable'}, {'name': 'expected_deaths', 'type': 'INTEGER', 'mode': 'nullable'}, {'name': 'excess_deaths', 'type': 'INTEGER', 'mode': 'nullable'}, {'name': 'baseline', 'type': 'STRING', 'mode': 'nullable'}],
    )

    excess_deaths_transform_csv >> load_excess_deaths_to_bq
    