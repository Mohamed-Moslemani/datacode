from gcp_dataLoad import Gcp
gcp_loader = Gcp(
    project_id="assessment-exercise1",
    dataset_id="Exercise1Data",
    csv_folder="data"
)

gcp_loader.clean_and_upload_csvs()         
gcp_loader.upload_csvs_with_autodetect()   
