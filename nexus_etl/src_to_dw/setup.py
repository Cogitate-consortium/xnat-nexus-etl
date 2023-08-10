import sys
import os

basedir = os.path.join(os.path.dirname(__file__), '..')

sys.path.append(os.path.join(basedir, 'common/utils'))
sys.path.append(os.path.join(basedir, 'common/dw_dataclasses'))
sys.path.append(os.path.join(basedir, 'sensitive'))

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

import xnatcred
xnat_server = xnatcred.server
xnat_username = xnatcred.username
xnat_password = xnatcred.password

