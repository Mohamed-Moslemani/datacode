from google.cloud import bigquery

class SchemaChecker:
    def __init__(self, project_id, dataset_id):
        self.client= bigquery.Client(project=project_id)
        self.dataset_ref= f"{project_id}.{dataset_id}"

    def check_all_schemas(self):
        tables= self.client.list_tables(self.dataset_ref)
        for table in tables:
            full_table_id= f"{self.dataset_ref}.{table.table_id}"
            table_obj = self.client.get_table(full_table_id)
            print(f"\n Table:{table.table_id}")
            for field in table_obj.schema:
                print(f" - {field.name} ({field.field_type})")
