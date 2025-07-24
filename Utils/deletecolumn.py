from google.cloud import bigquery
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Vars import project_id, dataset_id

def drop_column_bq(project_id: str, dataset_id: str, table_id: str, column_to_drop: str):
    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    table = client.get_table(full_table_id)
    existing_columns = [field.name for field in table.schema if field.name != column_to_drop]

    if column_to_drop not in [f.name for f in table.schema]:
        print(f"column `{column_to_drop}` does not exist in `{full_table_id}`.")
        return

    select_expr = ", ".join([f"`{col}`" for col in existing_columns])
    query = f"""
        CREATE OR REPLACE TABLE `{full_table_id}` AS
        SELECT {select_expr}
        FROM `{full_table_id}`
    """
    client.query(query).result()
    print(f" successfuly dropped column `{column_to_drop}` from `{full_table_id}`.")

drop_column_bq(project_id,dataset_id,"campaign_data2","Platform")