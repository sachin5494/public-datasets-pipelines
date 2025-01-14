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
    dag_id="iowa_liquor_sales.sales",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@weekly",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    transform_csv = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="transform_csv",
        startup_timeout_seconds=600,
        name="Sales",
        namespace="composer",
        service_account_name="datasets",
        image_pull_policy="Always",
        image="{{ var.json.iowa_liquor_sales.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URL": "https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv",
            "SOURCE_FILE": "files/data.csv",
            "TARGET_FILE": "files/data_output.csv",
            "CHUNKSIZE": "1000000",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/iowa_liquor_sales/sales/data_output.csv",
        },
        resources={"limit_memory": "8G", "limit_cpu": "3"},
    )

    # Task to load CSV data to a BigQuery table
    load_to_bq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id="load_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/iowa_liquor_sales/sales/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="iowa_liquor_sales.sales",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "invoice_and_item_number",
                "type": "STRING",
                "mode": "NULLABLE",
                "description": "Concatenated invoice and line number associated with the liquor order. This provides a unique identifier for the individual liquor products included in the store order.",
            },
            {
                "name": "date",
                "type": "DATE",
                "description": "Date of order",
                "mode": "NULLABLE",
            },
            {
                "name": "store_number",
                "type": "STRING",
                "description": "Unique number assigned to the store who ordered the liquor.",
                "mode": "NULLABLE",
            },
            {
                "name": "store_name",
                "type": "STRING",
                "description": "Name of store who ordered the liquor.",
                "mode": "NULLABLE",
            },
            {
                "name": "address",
                "type": "STRING",
                "description": "Address of store who ordered the liquor.",
                "mode": "NULLABLE",
            },
            {
                "name": "city",
                "type": "STRING",
                "description": "City where the store who ordered the liquor is located",
                "mode": "NULLABLE",
            },
            {
                "name": "zip_code",
                "type": "STRING",
                "description": "Zip code where the store who ordered the liquor is located",
                "mode": "NULLABLE",
            },
            {
                "name": "store_location",
                "type": "STRING",
                "description": "Location of store who ordered the liquor. The Address, City, State and Zip Code are geocoded to provide geographic coordinates. Accuracy of geocoding is dependent on how well the address is interpreted and the completeness of the reference data used.",
                "mode": "NULLABLE",
            },
            {
                "name": "county_number",
                "type": "STRING",
                "description": "Iowa county number for the county where store who ordered the liquor is located",
                "mode": "NULLABLE",
            },
            {
                "name": "county",
                "type": "STRING",
                "description": "County where the store who ordered the liquor is located",
                "mode": "NULLABLE",
            },
            {
                "name": "category",
                "type": "STRING",
                "description": "Category code associated with the liquor ordered",
                "mode": "NULLABLE",
            },
            {
                "name": "category_name",
                "type": "STRING",
                "description": "Category of the liquor ordered.",
                "mode": "NULLABLE",
            },
            {
                "name": "vendor_number",
                "type": "STRING",
                "description": "The vendor number of the company for the brand of liquor ordered",
                "mode": "NULLABLE",
            },
            {
                "name": "vendor_name",
                "type": "STRING",
                "description": "The vendor name of the company for the brand of liquor ordered",
                "mode": "NULLABLE",
            },
            {
                "name": "item_number",
                "type": "STRING",
                "description": "Item number for the individual liquor product ordered.",
                "mode": "NULLABLE",
            },
            {
                "name": "item_description",
                "type": "STRING",
                "description": "Description of the individual liquor product ordered.",
                "mode": "NULLABLE",
            },
            {
                "name": "pack",
                "type": "INTEGER",
                "description": "The number of bottles in a case for the liquor ordered",
                "mode": "NULLABLE",
            },
            {
                "name": "bottle_volume_ml",
                "type": "INTEGER",
                "description": "Volume of each liquor bottle ordered in milliliters.",
                "mode": "NULLABLE",
            },
            {
                "name": "state_bottle_cost",
                "type": "FLOAT",
                "description": "The amount that Alcoholic Beverages Division paid for each bottle of liquor ordered",
                "mode": "NULLABLE",
            },
            {
                "name": "state_bottle_retail",
                "type": "FLOAT",
                "description": "The amount the store paid for each bottle of liquor ordered",
                "mode": "NULLABLE",
            },
            {
                "name": "bottles_sold",
                "type": "INTEGER",
                "description": "The number of bottles of liquor ordered by the store",
                "mode": "NULLABLE",
            },
            {
                "name": "sale_dollars",
                "type": "FLOAT",
                "description": "Total cost of liquor order (number of bottles multiplied by the state bottle retail)",
                "mode": "NULLABLE",
            },
            {
                "name": "volume_sold_liters",
                "type": "FLOAT",
                "description": 'Total volume of liquor ordered in liters. (i.e. (Bottle Volume (ml) x Bottles Sold)/1,000)"',
                "mode": "NULLABLE",
            },
            {
                "name": "volume_sold_gallons",
                "type": "FLOAT",
                "description": 'Total volume of liquor ordered in gallons. (i.e. (Bottle Volume (ml) x Bottles Sold)/3785.411784)"',
                "mode": "NULLABLE",
            },
        ],
    )

    transform_csv >> load_to_bq
