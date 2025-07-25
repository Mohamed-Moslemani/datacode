import pandas as pd
import os
from google.cloud import bigquery
import re

##dwefining the class to load the data into the data warehouse 
class Gcp:
    def __init__(self, project_id, dataset_id, csv_folder):
        self.project_id= project_id
        self.dataset_id= dataset_id
        self.csv_folder= csv_folder
        self.client= bigquery.Client(project=project_id)
        self.dataset_ref= "{}.{}".format(project_id,dataset_id)
        
##function that deals with the names of the columns that caused error: name not supported by current character map - preferred manually setting names than using the v2
    def clean_and_upload_csvs(self):
        for filename in os.listdir(self.csv_folder):
            if filename.endswith(".csv"):
                file_path = os.path.join(self.csv_folder, filename)
                df = pd.read_csv(file_path)
                df.columns = [
                    col.strip()
                    .replace(" ", "_")
                    .replace("(", "")
                    .replace(")", "")
                    .replace("$", "")
                    .replace("/", "_")
                    .replace("-", "_")
                    for col in df.columns
                ]
                df.to_csv(file_path, index=False)
                table_name = filename.replace(".csv", "")
                table_id = f"{self.dataset_ref}.{table_name}"

                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    autodetect=True,
                    write_disposition="WRITE_TRUNCATE",
                )

                with open(file_path,"rb") as source_file:
                    load_job = self.client.load_table_from_file(
                        source_file, table_id, job_config=job_config
                    )
                    load_job.result()
                    print(f"Uploaded siuccesfully (cleaned): {table_id} ({load_job.output_rows} rows)")

    def upload_csvs_with_autodetect(self):
        for filename in os.listdir(self.csv_folder):
            if filename.endswith(".csv"):
                table_name = filename.replace(".csv", "")
                table_id= f"{self.dataset_ref}.{table_name}"
                file_path= os.path.join(self.csv_folder, filename)

                job_config= bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    autodetect=True,
                    allow_jagged_rows=True,
                    allow_quoted_newlines=True,
                    quote_character='"',
                    field_delimiter=",",
                    ignore_unknown_values=True,
                    write_disposition="WRITE_TRUNCATE",
                    use_avro_logical_types=True,
                )

                with open(file_path, "rb") as source_file:
                    load_job = self.client.load_table_from_file(
                        source_file,table_id,job_config=job_config
                    )
                    load_job.result()
                    print("Uploaded succesfull: {} ({} rows)".format(table_id,load_job.output_rows))


    def merge_and_upload_to_single_table(self, table_name, partition_field):
        df_list = []
        for filename in os.listdir("data/adsData"):
            if not filename.endswith(".csv"):
                continue

            file_path = os.path.join("data/adsData", filename)
            df = pd.read_csv(file_path)


            match = re.match(r"([A-Za-z0-9]+)_", filename)
            df["Company"] = match.group(1) if match else "Unknown"
            df_list.append(df)

        if not df_list:
            print("no CSV files found to merge in folder.")
            return

        merged_df = pd.concat(df_list, ignore_index=True)

        dest_table= f"{self.dataset_ref}.{table_name}"
        job_config= bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )

        load_job = self.client.load_table_from_dataframe(
            merged_df, dest_table, job_config=job_config
        )
        load_job.result()

        print(f"merged and uploaded {load_job.output_rows} rows to {dest_table}")