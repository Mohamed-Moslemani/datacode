from google.cloud import bigquery
from Taxonomy.TaxonomyParser import TaxonomyParser

class BQTaxonomyProcessor:
    def __init__(self, project_id, dataset_id, table_list):
        self.client = bigquery.Client(project=project_id)
        self.project_id= project_id
        self.dataset_id= dataset_id
        self.table_list = table_list
        self.parser= TaxonomyParser()
    def run(self):
        for table_name in self.table_list:
            full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            print(f"Processin: {full_table_id}")

            df = self.client.query(f"SELECT * FROM `{full_table_id}`").to_dataframe()
            parsed_df = self.parser.parse_dataframe(df)

            parsed_table_name = f"{table_name}_parsed"
            dest_table = f"{self.dataset_id}.{parsed_table_name}"

            job= self.client.load_table_from_dataframe(
                parsed_df,
                destination=dest_table,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            )
            job.result()
            print(f"success Written to: {dest_table}")
