import functions_framework
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
from flask import jsonify
import threading
import gc

@functions_framework.http
def http_handler(request):
    threading.Thread(target=merge).start()
    return {"status": "Started processing"}, 200

def merge():
    # Define your project and dataset
    project_id = 'XXXXXXXXXX'
    dataset_id = 'AppStore'
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the schema and merge process for each type of data (daily, weekly, monthly)
    merge_jobs = [
        {
            "table_prefix": "ONGOING_AppDownloadsDetailed_",
            "variable_field": ["DAILY", "WEEKLY", "MONTHLY"],
            "staging_table": f"{project_id}.AppStoreMerged.Merged_AppDownloadsDetailed_{{}}",
            "schema": [
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("Download_Type", "STRING"),
                bigquery.SchemaField("Device", "STRING"),
                bigquery.SchemaField("Source_Type", "STRING"),
                bigquery.SchemaField("Source_Info", "STRING"),
                bigquery.SchemaField("Territory", "STRING"),
                bigquery.SchemaField("Counts", "INTEGER")
            ],
            "query": """
            SELECT
                created_at,
                `Download Type` as Download_Type,
                Device,
                `Source Type` as Source_Type,
                `Source Info` as Source_Info,
                Territory,
                Counts
            FROM `{table_name}`
            """
        },
        {
            "table_prefix": "ONGOING_AppStoreDiscoveryandEngagementDetailed_",
            "variable_field": ["DAILY", "WEEKLY", "MONTHLY"],
            "staging_table": f"{project_id}.AppStoreMerged.Merged_AppStoreDiscoveryandEngagementDetailed_{{}}",
            "schema": [
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("Event", "STRING"),
                bigquery.SchemaField("Engagement_Type", "STRING"),
                bigquery.SchemaField("Device", "STRING"),
                bigquery.SchemaField("Territory", "STRING"),
                bigquery.SchemaField("Counts", "INTEGER"),
                bigquery.SchemaField("Unique_Counts", "INTEGER")
            ],
            "query": """
            SELECT
                created_at,
                Event,
                `Engagement Type` as Engagement_Type,
                Device,
                Territory,
                Counts,
                `Unique Counts` as Unique_Counts
            FROM `{table_name}`
            """
        },
        {
            "table_prefix": "ONGOING_AppStoreInstallationandDeletionDetailed_",
            "variable_field": ["DAILY", "WEEKLY", "MONTHLY"],
            "staging_table": f"{project_id}.AppStoreMerged.Merged_AppStoreInstallationandDeletionDetailed_{{}}",
            "schema": [
                bigquery.SchemaField("created_at", "STRING"),
                bigquery.SchemaField("Event", "STRING"),
                bigquery.SchemaField("Download_Type", "STRING"),
                bigquery.SchemaField("Source_Type", "STRING"),
                bigquery.SchemaField("Territory", "STRING"),
                bigquery.SchemaField("Counts", "INTEGER"),
                bigquery.SchemaField("Unique_Devices", "INTEGER")
            ],
            "query": """
            SELECT
                created_at,
                Event,
                `Download Type` as Download_Type,
                `Source Type` as Source_Type,
                Territory,
                Counts,
                `Unique Devices` as Unique_Devices
            FROM `{table_name}`
            """
        }
    ]

    # Get all tables in the dataset
    all_tables = client.list_tables(f"{project_id}.{dataset_id}")
    table_names = {table.table_id for table in all_tables}

    # Process each job
    for job in merge_jobs:
        for variable_field in job["variable_field"]:
            staging_table = job["staging_table"].format(variable_field)
            
            # Define date ranges by frequency type
            start_date = datetime.strptime("20240705", '%Y%m%d')
            end_date = datetime.now()

            # Generate dates according to the specific interval pattern
            if variable_field == "DAILY":
                dates = pd.date_range(start=start_date, end=end_date, freq='D')
            elif variable_field == "WEEKLY":
                dates = pd.date_range(start=start_date, end=end_date, freq='W-FRI')
            elif variable_field == "MONTHLY":
                dates = pd.date_range(start=start_date, end=end_date, freq='MS') + timedelta(days=4)

            # Process in smaller batches to optimize memory usage
            batch_size = 10  # Adjust batch size for testing
            for i in range(0, len(dates), batch_size):
                batch_dates = dates[i:i + batch_size]

                # Build UNION ALL query with the batched dates
                union_queries = []
                for date in batch_dates:
                    table_name = f"{project_id}.{dataset_id}.{job['table_prefix']}{variable_field}_{date.strftime('%Y%m%d')}"
                    union_queries.append(f"(SELECT * FROM `{table_name}`)")

                # Merge batch of tables in BigQuery and replace the staging table
                final_query = f"""
                CREATE OR REPLACE TABLE `{staging_table}` AS
                {" UNION ALL ".join(union_queries)}
                """
                
                # Execute batch query
                client.query(final_query).result()
                print(f"Data batch merged successfully into {staging_table} for {variable_field} range {i}-{i + batch_size - 1}")

                # Release memory by clearing variables and forcing garbage collection
                del union_queries, batch_dates
                gc.collect()

    print("Data merging complete. All data stored in respective staging tables.")