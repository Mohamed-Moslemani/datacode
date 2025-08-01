from google.cloud import bigquery

def compute_cpa(project_id: str, dataset_id: str,
                input_table: str, output_table: str):
    client = bigquery.Client(project=project_id)
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{output_table}` AS
    SELECT *,
           SAFE_DIVIDE(Spend_USD, Conversions) AS CPA
    FROM `{project_id}.{dataset_id}.{input_table}`
    """
    client.query(query).result()
    print(f"cpa computed and saved to `{output_table}`")
