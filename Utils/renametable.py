from google.cloud import bigquery
import sys,os 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Vars import project_id, dataset_id 

def rename_table_bq(project_id, dataset_id, old_table_name, new_table_name):
    client= bigquery.Client(project=project_id)

    source=f"{project_id}.{dataset_id}.{old_table_name}"
    destination= f"{project_id}.{dataset_id}.{new_table_name}"
    job=client.copy_table(source, destination)
    job.result()  

    client.delete_table(source, not_found_ok=True)
    print(f"Renamed table `{old_table_name}` → `{new_table_name}`")


rename_table_bq(project_id,dataset_id,"campaign_budget_joined","cleaned_dataset")