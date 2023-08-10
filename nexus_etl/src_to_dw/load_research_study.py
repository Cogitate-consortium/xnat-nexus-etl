import pandas as pd
import sys
import setup
import json
import os
from datetime import datetime
from get_xnat_data import get_project_data
config = setup.config

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table
from research_study import research_study

engine = connect_to_db()

def extract_project_data(project_resource: dict) -> pd.DataFrame:
    """
    Extracts project data from a previously fetched XNAT Project metadata
    and converts it into a DataFrame.

    This function takes in a dictionary containing project data and returns a DataFrame 
    containing the same data with specific types based on 'research_study' table.

    Args:
    project_resource (dict): The input dictionary containing project data.

    Returns:
    pd.DataFrame: The DataFrame containing the project data.
    """
    
    # Create a research_study object from the project_resource data
    xnat_project = research_study(
        src_system= "MPG XNAT",
        research_study_id=project_resource['xnat:Project']['@ID'],
        research_study_id_type='xnat_project_id',
        research_study_title=project_resource['xnat:Project']['xnat:name'],
        research_study_description=project_resource['xnat:Project'].get('xnat:description', None),
        research_study_site=project_resource['xnat:Project'].get('xnat:acquisition_site', None),
        research_study_status=project_resource['xnat:Project'].get('@active', None),
        xnat_custom_fields=json.dumps(json.loads(project_resource['xnat:Project'].get('xnat:custom_fields', '{}')))
    )

    # Convert the xnat_project object into a DataFrame
    project_df = pd.DataFrame.from_dict([xnat_project])

    # Replace empty strings with None in the DataFrame
    project_df = project_df.replace({"": None})

    # Convert the data types in the DataFrame based on the research_study table
    project_df = convert_datatypes_based_on_table('research_study', project_df)

    return project_df


def delete_existing_data(research_study_id: str) -> None:
    """
    Deletes existing project data from the research_study table.

    This function deletes rows from the research_study table that have the same research_study_id as the input.

    Args:
    research_study_id (str): The id of the research study to be deleted.

    Returns:
    None
    """

    # SQL statement to delete the data from research_study table
    delete_stmt = f"""
                    delete
                    from 
                        research_study
                    where 
                        research_study_id_type = 'xnat_project_id'
                        and research_study_id = '{research_study_id}'
                """
    
    # Execute the delete statement
    cursor = engine.cursor()
    try:
        cursor.execute(delete_stmt)
        engine.commit()
    except Exception as error:
        error_string = str(error)
        if "no such table" in error_string:
            None
        else:
            raise error


def get_existing_data(research_study_id: str) -> pd.DataFrame:
    """
    Gets existing project data from the research_study table.

    This function retrieves rows from the research_study table that have the same research_study_id as the input.

    Args:
    research_study_id (str): The id of the research study to retrieve.

    Returns:
    pd.DataFrame: The DataFrame containing the existing project data.
    """

    # SQL query to get the data from research_study table
    query = f"""
                select * 
                from 
                    research_study
                where 
                    research_study_id = '{research_study_id}'
                    and research_study_id_type = 'xnat_project_id'
            """
    # Execute the query and save the result in a DataFrame
    existing_df = pd.read_sql(query, engine)

    # Convert the data types in the DataFrame based on the research_study table
    existing_df = convert_datatypes_based_on_table('research_study', existing_df) 

    return existing_df


def main():
    """
    The main function of the script.

    It retrieves project metadata, compares it with existing data, calculates the delta, and loads it to the Postgres database.

    This function is called when the script is run from the command line.

    Returns:
    None
    """
    
    project_id = sys.argv[1]
    processing_date = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    
    # get the project metadata
    project_resource = get_project_data(project_id)

    # extract the project data from the project metadata
    project_df = extract_project_data(project_resource)

    # get the existing project data
    existing_df = get_existing_data(project_id)

    # calculate the delta between the existing and the new project data
    delta_df = calculate_delta(
        existing_df, 
        project_df, 
        'research_study_uri', 
        [
            'research_study_id',
            'research_study_id_type'
        ]
    )

    # parse the delta results
    load_df = parse_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = processing_date, 
        uri_field_name = 'research_study_uri', 
        delta_df = delta_df,
        generate_uri_flag=True,
        uri_salt_field_list = ['research_study_id','research_study_id_type']
    )

    if len(load_df) > 0:

        load_df = convert_datatypes_based_on_table('research_study', load_df)

        # delete the existing data
        delete_existing_data(project_id)

        # load the new data to Postgres
        load_df.to_sql(
            'research_study',
            engine,
            if_exists='append',
            index=False
        )
    

if __name__ == "__main__":
    main()
