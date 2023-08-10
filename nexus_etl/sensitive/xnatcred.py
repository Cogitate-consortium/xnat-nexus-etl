import json
import io
import os

caller_dir = os.getcwd()
this_dir = os.path.dirname(__file__)
os.chdir(this_dir)

server = ''
username = ''
password = ''

# Load the configuration file
with open("xnat_config.cfg") as json_data_file:
    data = json.load(json_data_file)
    server = data['server']
    username = data['user']
    password = data['password']

# reset the working directory
os.chdir(caller_dir)
