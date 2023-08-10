import os
import sys
import logging
import nexussdk as nexus
from nexussdk import client
from SPARQLWrapper import JSON, POST

basedir = os.path.join(os.path.dirname(__file__), '..')

sys.path.append(os.path.join(basedir, 'common/utils'))
sys.path.append(os.path.join(basedir, 'common/dw_dataclasses'))
sys.path.append(os.path.join(basedir, 'sensitive'))

# sys.path.insert(1, '../common/utils')
# sys.path.insert(1, '../common/dw_dataclasses')
# sys.path.insert(1, '../sensitive')

# read the configuration file
from DataEngineeringConfig import read_yaml_file

yaml_file_list = [
    os.path.join(basedir, 'common/config/env_config.yaml'),
    os.path.join(basedir, 'common/config/common_config.yaml')
]

config = read_yaml_file(yaml_file_list)

# second we need to make the folders for the dependent scripts
# part of the path
for dir in config['server']['common_utils_path']:
    sys.path.insert(1, dir)

# import additional packages after setting dependency script path
import NexusSparqlQuery as qns

# configure logging
logging.basicConfig(
    filename=f'../logs/testing_with_nexus.log',
    filemode='a+',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

# set nexus org and project
nexus_deployment = config['nexus']['deployment']
nexus_org = config['nexus']['org']
nexus_project = config['nexus']['project']
token = config['nexus']['token_file']
f = open(token, 'r')
token = f.read().rstrip()

# create nexus connection
nexus = client.NexusClient(nexus_deployment, token)
nexus.permissions.fetch()

sparqlview_endpoint = nexus_deployment+"/views/"+nexus_org+"/"+nexus_project+"/graph/sparql"

sparqlview_wrapper = qns.create_sparql_client(
    sparql_endpoint=sparqlview_endpoint,
    token=token,
    http_query_method= POST, 
    result_format=JSON
)
