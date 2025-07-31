from google.cloud import bigquery

class SchemaSetter:
    def __init__(self, project_id, dataset_id):
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = f"{project_id}.{dataset_id}"

    def create_table_with_schema(self, table_name, schema_fields):
        table_id= f"{self.dataset_ref}.{table_name}"
        table = bigquery.Table(table_id, schema=schema_fields)
        table = self.client.create_table(table, exists_ok=True)
        print(f"Created/Updated table in success: {table_id}")