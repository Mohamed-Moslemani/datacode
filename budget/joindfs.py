from google.cloud import bigquery

def join_campaign_with_budget_bq(project_id: str, dataset_id: str,campaign_table: str, budget_table: str,output_table: str):
    client = bigquery.Client(project=project_id)
    joined_query= f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{output_table}` AS
    SELECT 
        c.*, 
        b.Planned_Daily_Budget_USD
    FROM `{project_id}.{dataset_id}.{campaign_table}` AS c
    LEFT JOIN `{project_id}.{dataset_id}.{budget_table}` AS b
    ON c.BusinessUnit= b.BusinessUnit
       AND c.Objective= b.Objective
       AND c.Market= b.Market
       AND c.Language=b.Language
       AND c.Audience=b.Audience
       AND c.Channel= b.Channel
       AND c.Month=b.Month
       AND IFNULL(c.Route, 'NA')=IFNULL(b.Route, 'NA')
       AND DATE(c.Date)= DATE(b.Date)
    """

    client.query(joined_query).result()
    print(f"oined table written to `{dataset_id}.{output_table}`")
