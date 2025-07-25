from google.cloud import bigquery
import os,sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Vars import project_id, dataset_id

def list_bq_tables(project_id: str, dataset_id: str):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    tables = client.list_tables(dataset_ref)
    print(f"Tables in dataset `{dataset_id}`:")
    for table in tables:
        print(f"-{table.table_id}")


def check_validity_summary():
    client = bigquery.Client(project=project_id)
    namedataset = input("Enter name of the dataset: ").strip()

    validity_query = f"""
        SELECT COUNT(*) AS valid_count
        FROM `{project_id}.{dataset_id}.{namedataset}`
        WHERE Validity = TRUE
    """
    totalrows_query = f"""
        SELECT COUNT(*) AS total_rows
        FROM `{project_id}.{dataset_id}.{namedataset}`
    """
    headquery = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{namedataset}`
        LIMIT 5
    """
    df_validity= client.query(validity_query).to_dataframe()
    df_total = client.query(totalrows_query).to_dataframe()
    df_head = client.query(headquery).to_dataframe()

    print("===============================")
    print("sample Rows:")
    print(df_head)
    print("===============================")
    print("total Rows:")
    print(df_total)
    print("=============================")
    print("valididty check:")
    print(df_validity)

list_bq_tables(project_id,dataset_id)