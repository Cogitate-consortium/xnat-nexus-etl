import pandas as pd
import setup
import nexussdk as nexus
import json
config = setup.config

from DbConnection import connect_to_db
import NexusSparqlQuery as qns

engine = connect_to_db()

nexus_deployment = setup.nexus_deployment
org = setup.nexus_org
project = setup.nexus_project
token = setup.token

sparqlview_endpoint = setup.sparqlview_endpoint
sparqlview_wrapper = setup.sparqlview_wrapper

# create nexus connection
nexus = setup.nexus

# Opening JSON file
f = open('resource_deprecation_mapping.json')
resource_mapping_dict = json.load(f)
resource_mapping_dict

for resource_mapping in resource_mapping_dict:
    
    print(resource_mapping['postgres_table_name'])
    
    # let's get a list of all research studies from postgres
    query = f"""
        select 
            distinct {resource_mapping['postgres_uri_field_name']} resource_uri 
        from 
            mpg_eln_dev.{resource_mapping['postgres_table_name']}
        where
            {resource_mapping['postgres_uri_field_name']} is not null
    """
    pg_df = pd.read_sql(query, engine)
    print(f"Length of postgres query: {len(pg_df)}")
    
    query_prefix = """
        prefix nxv: <https://bluebrain.github.io/nexus/vocabulary/>
        prefix fhir: <http://hl7.org/fhir/>
        prefix nidm: <http://purl.org/nidash/nidm#>
        prefix sdo: <https://schema.org/>
        
    """
    
    if resource_mapping.get('nexus_resource_type_regex', False):
        
        print(resource_mapping['nexus_resource_type_regex'])
        
        query_body = f"""
        
            select * where {{
                ?resource_uri nxv:deprecated false .
                FILTER NOT EXISTS {{ ?resource_uri nxv:deprecated true }} .
                ?resource_uri a ?resource_type .
                FILTER (regex(str(?resource_type), "{resource_mapping['nexus_resource_type']}", "i")) .
            }}
            
        """
    else:
        query_body = f"""
        
            select * where {{
                ?resource_uri nxv:deprecated false .
                FILTER NOT EXISTS {{ ?resource_uri nxv:deprecated true }} .
                ?resource_uri a {resource_mapping['nexus_resource_type']} .
            }}
            
        """
    
    sparql_query = query_prefix + query_body
    
    df = qns.query_sparql(sparql_query,sparqlview_wrapper)
    df = qns.sparql2dataframe(df)
    print(f"Length of sparql query: {len(df)}")
    
    # join the data frames
    join_df = pg_df.merge(
        df,
        how = 'right',
        on = 'resource_uri',
        indicator = True
    )

    # delete resources that are only in Nexus
    nexus_only_df = join_df.loc[join_df['_merge']=='right_only']
    print(f"Resources to delete: {len(nexus_only_df)}")
          
    for index, row in nexus_only_df.iterrows():
        resource = nexus.resources.fetch(org, project, row['resource_uri'])
        print(resource["@id"])
        
        try:
            nexus.resources.deprecate(resource)      
        except:
            print("Could not deprecate")
        