from google.cloud import bigquery
from Vars import project_id,dataset_id

####testing on the parsed dataset, whether validity varies, apparently its not varying, either all are valid, or all are not valid (depending on whether the FA is a typo)
client = bigquery.Client(project=project_id)
namedataset= input("Enter name of the dataset: ")
validityquery= f"""
SELECT COUNT(*) AS invalid_count
FROM `{project_id}.{dataset_id}.{namedataset}`
WHERE Validity = True
"""
totalRowsquery=f"""
SELECT COUNT(*) AS total_rows
FROM `{project_id}.{dataset_id}.{namedataset}`
"""

head= f"""
SELECT *
FROM `{project_id}.{dataset_id}.{namedataset}`
LIMIT 5
"""

df = client.query(validityquery).to_dataframe()
df2= client.query(head).to_dataframe()
df3= client.query(totalRowsquery).to_dataframe()
print(df)
print("===============================")
print(df2)
print("=======================")
print(df3)