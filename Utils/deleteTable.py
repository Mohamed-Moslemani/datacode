from google.cloud import bigquery
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Vars import project_id, dataset_id

client = bigquery.Client(project=project_id)

tables_to_delete = [
    f"{project_id}.{dataset_id}.campaign_data2"
]

for table_id in tables_to_delete:
    client.delete_table(table_id, not_found_ok=True)
    print(f"Deleted: {table_id}")
