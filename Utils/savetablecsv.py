from google.cloud import bigquery
import pandas as pd
import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Vars import project_id,dataset_id
def save_bq_table_as_csv(project_id: str, dataset_id: str, table_id: str, output_path: str):
    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    query= f"SELECT * FROM `{full_table_id}`"
    df= client.query(query).to_dataframe()
    df.to_csv(output_path, index=False)
    print(f"table `{full_table_id}` saved to `{output_path}`")

save_bq_table_as_csv(project_id,dataset_id,"campaign_data2_parsed","/home//mohamed//assessment//exercise1//data//savedData//campaign_data2_parsed.csv")
