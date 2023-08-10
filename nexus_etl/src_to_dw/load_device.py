import pandas as pd
import os
import sys
from pyxnat import Interface
from get_xnat_data import get_acquisition_data, get_session_datatypes
import setup
from datetime import datetime
config = setup.config

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table

basedir = os.path.join(os.path.dirname(__file__), '..')

# Establishing database and xnat connections
engine = connect_to_db()
interface = Interface(config = os.path.join(basedir, 'sensitive/xnat_config.cfg'))
# interface = Interface(config = '../sensitive/xnat_config.cfg')

nifi_proc_dt = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
experiment_type = None
# Fetching the types of session data available
experiment_type_list = get_session_datatypes()

# Initialize list to store device data
data_list = []

def delete_existing_data() -> None:
    """
    Truncates the 'device' table in the database.

    Raises:
        error: If the SQL execution fails for a reason other than the table not existing.
    """
    # Prepare the SQL statement to truncate the 'device' table
    delete_stmt = f"""
                    delete from device
                """

    cursor = engine.cursor()

    try:
        # Attempt to execute the SQL statement
        cursor.execute(delete_stmt)
        engine.commit()

    except Exception as error:
        error_string = str(error)
        # Ignore errors related to the table not existing
        if "no such table" not in error_string:
            raise error


# Prepare the SQL statement to fetch project IDs
query = f"""select xnat_project_id from xnat_config"""
# Execute the SQL statement and convert the results to a list
project_list = pd.read_sql(query, engine)['xnat_project_id'].to_list()

for project_id in project_list:

    for xnat_experiment_type in experiment_type_list:
        
        # Fetch acquisition data for the current project and experiment type
        acquisition_list = get_acquisition_data(project_id=project_id, xnat_experiment_type=xnat_experiment_type)

        for scan_data in acquisition_list:
            # Prepare a dictionary of device details for each scan
            scan_dict = {
                'src_system': "MPG XNAT",
                'device_manufacturer': scan_data['xnat:imagescandata/scanner/manufacturer'],
                'device_name': scan_data['xnat:imagescandata/scanner/model']
            }
            # Add the dictionary to the data list
            data_list.append(scan_dict)

# Transform the list of dictionaries into a DataFrame
scan_df = pd.DataFrame.from_dict(data_list)
# Replace empty strings in the DataFrame with None
scan_df = scan_df.replace({"": None})  
# Remove duplicate rows from the DataFrame
scan_df = scan_df.drop_duplicates()

# Prepare the SQL statement to fetch existing device data
query = f"""
            select * 
            from 
                device
        """
# Execute the SQL statement and store the results in a DataFrame
existing_df = pd.read_sql(query, engine)

# Convert data types in the scan and existing DataFrames based on the 'device' table schema
scan_df = convert_datatypes_based_on_table('device', scan_df)
existing_df = convert_datatypes_based_on_table('device', existing_df)

# Calculate the delta between the existing and new data
delta_df = calculate_delta(
    existing_df, 
    scan_df, 
    'device_uri', 
    [
        'src_system',
        'device_manufacturer',
        'device_name'
    ]
)

# Parse the results of the delta calculation
load_df = parse_delta_results(
    nexus_base = config['nexus']['uri_base'], 
    proc_dt = nifi_proc_dt, 
    uri_field_name = 'device_uri', 
    delta_df = delta_df,
    uri_salt_field_list = [
        'src_system',
        'device_manufacturer',
        'device_name'
    ]
)

# If there is data to load, delete the existing data and insert the new data
if len(load_df) > 0:

    delete_existing_data()

    # Convert data types in the load DataFrame based on the 'device' table schema
    load_df = convert_datatypes_based_on_table('device', load_df)

    # Insert the new data into the 'device' table
    load_df.to_sql('device', engine, if_exists='append', index=False)
