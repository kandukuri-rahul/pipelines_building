
from loadDynamo import load_to_Dynamo
from fetch_from_dynamoDB import fetch_data_from_dynamo
from loadtoSQL import load_to_ssms

 

fetched_data = fetch_data_from_dynamo("pipeline")
load_to_ssms(fetched_data)

 
# #read data from mongodb
# mongo_data = read_data()
 
# #loading mongo_data to dynamodb
# load_data_to_dynamoDB('Projects',mongo_data)
 
# #fetch data from dynamodb
# fetched_data = fetch_data_from_dynamo('Projects') 
 
# #load dynamodb data to sqlserver
# load_to_ssms(fetched_data)
