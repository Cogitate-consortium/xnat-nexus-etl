import pandas as pd
import sys
import setup
import nexussdk as nexus
from urllib.request import urlopen
from SPARQLWrapper import JSON, POST
import json
import numpy as np
config = setup.config
import datetime
from DbConnection import connect_to_db
from DatatypeConverter import convert_datatypes_based_on_table
from dataclasses import fields
import NexusSparqlQuery as qns
from dw_dataclasses.device import device

engine = connect_to_db()

# set some parameters to define which org and project we are loading to in nexus
# also create the nexus connection object
nexus_deployment = config['nexus']['deployment']
org = config['nexus']['org']
project = config['nexus']['project']
token = config['nexus']['token_file']
nexus = setup.nexus

f = open(token, 'r')
token = f.read().rstrip()

sparqlview_endpoint = nexus_deployment+"/views/"+org+"/"+project+"/graph/sparql"
sparqlview_wrapper = qns.create_sparql_client(sparql_endpoint=sparqlview_endpoint, token=token,http_query_method= POST, result_format=JSON)



def validate_structure(dataclass_definition, df):
    """
    Creating a function to make sure that the data structure 
    we're processing matches the dataclass structure defined
    """
    
    attributes = [field.name for field in fields(dataclass_definition)]
    assert set(list(attributes))==set(list(df.columns)), (
        f"Data does not have expected structure.  See dataclass definition {dataclass_definition}")
    
    
# set parameters for the script depending on what data is being processed
postgres_table_name = 'device'
resource_type = 'fhir:Device'
last_processed_date = '1990-01-01 00:00:00' 


# get the postgres data
query = f"""
            select * from {postgres_table_name}
            where "_updatedat" > 
                coalesce (
                    (select max(last_success_load_ts) from nexus_etl_log
                    where resource_type = '{resource_type}'),
                    '{last_processed_date}'
            )
        """
df = pd.read_sql(query, engine)
df = convert_datatypes_based_on_table(postgres_table_name, df) 
df = df.replace({np.nan:None})

current_ts = datetime.datetime.now()

# validate that the data structure matches the dataclass defn
validate_structure(device, df)

# import the context needed for nexus
url = "https://bluebrainnexus.io/contexts/metadata.json"
response = urlopen(url)
metadata_dict = json.loads(response.read())


for index, row in df.iterrows():
    
    dc = device()
    
    for column in df.columns:
        setattr(dc, column, row[column])

    dc_dict = dc.nexus_resource_constructor()
    
    try:
        nexus_dict = nexus.resources.fetch(org, project, row.device_uri)
    except:
        nexus_dict = None
        
    if nexus_dict:
        print("Updating")
        dc_dict['_self'] = nexus_dict['_self']
        print(json.dumps(dc_dict, indent=4))
        nexus.resources.update(dc_dict, nexus_dict['_rev'])
        
    else:
        print("Inserting")
        nexus.resources.create(org, project, dc_dict)
    
# updating the nexus_etl_log table
df = pd.DataFrame([[resource_type, current_ts]], columns=['resource_type', 'last_success_load_ts'])
df.to_sql('nexus_etl_log', engine, if_exists='append', index=False)