from Utils import gcp_dataLoad
from schema import schema_checker
from schema import schema_setter
from Taxonomy import TaxonomyParser
from Taxonomy import TaxonomyRunner
import os 
from Vars import project_id,dataset_id
from budget import joindfs, cpa, pacingPercent

####GCP LOADER THAT LOADED THE DATA IMTO THE BQ. THIS WAS THE FIRST RUN. 
###each csv file is on its own table
# gcp_loader = Gcp(
#     project_id="assessment-exercise1",
#     dataset_id="Exercise1Data",
#     csv_folder="data"
# )
# gcp_loader.clean_and_upload_csvs()         
# gcp_loader.upload_csvs_with_autodetect()   

#_____________________________________________________________________________________________

###UPLOADING ALL INTO A SINGLE TABLE. THIS IS THE SECOND RUN
# gg = Gcp(project_id, dataset_id,"data//adsData")
# gg.merge_and_upload_to_single_table(
#     table_name='campaign_data2',
#     partition_field='Date',
# )

#_____________________________________________________________________________________________


###SCHEMA CHECKER OF THE TABLES. THIS WAS THE THIRd RUN
# checker= schema_checker.SchemaChecker(project_id, dataset_id)
# checker.check_all_schemas()


#_____________________________________________________________________________________________


######TAXONOMY PARSER THAT PARSED THE CAMPIAGN NAMES. THIS WAS THE FOURTH RUN
#### No invalid campaign names inside all the datasets, either FA is a typo inside the assessment, hence, all of the campaign names will be invalid since the yall start wiuth AN, or its not, and all are valid, since all the other rules work perfectly fine, hence, no isolation for any campaignname.




#####TAXONOMY PARSER ON THE JOInED DATASET. THIS IS THE FIFTH rUN
# table_list = ["campaign_data2"]
# processor = TaxonomyRunner.BQTaxonomyProcessor(project_id, dataset_id, table_list)
# processor.run()

#_____________________________________________________________________________________________


#######JOINING MAIN DF WITH THE BUDGET PACING DF.THIS IS THE SIXTH RUN
##this uses the budget joining function in the budget module ot join the parsed df with the budget pacing df according to the predictors mentioned.
# joindfs.join_campaign_with_budget_bq(
#     project_id="assessment-exercise1",
#     dataset_id="Exercise1Data",
#     campaign_table="campaign_data2_parsed",
#     budget_table="Budget_Pacing",
#     output_table="campaign_budget_joined"
# )

#______________________________________________________
####COMPUTING PACING AND CPA AS REQUESTED THE ROAS CANNOT BE COMPUTED AS NO REVENUE IS FOUND AND REVENUE CANNOT BE COMPUTED OF HTE AVAILABLE PREDICTORS
# joinedf= "campaign_budget_joined"

# pacingPercent.compute_pacing_percent(
#     project_id="assessment-exercise1",
#     dataset_id="Exercise1Data",
#     input_table=joinedf,
#     output_table=joinedf
# )

# cpa.compute_cpa(
#     project_id="assessment-exercise1",
#     dataset_id="Exercise1Data",
#     input_table=joinedf,
#     output_table=joinedf
# )
