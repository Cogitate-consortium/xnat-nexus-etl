import pandas as pd
import sys
from get_xnat_data import get_subject_data, get_subject_list
import setup
import json
from datetime import datetime
config = setup.config

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table
from dw_dataclasses.research_subject import research_subject

engine = connect_to_db()
pk_fields = [
            'src_system',
            'research_subject_id',
            'research_study_id',
            'research_study_id_type'
        ]

def delete_existing_data(project_id: str, subject_id: str) -> None:
    """
    Deletes existing data from the research_subject table. The deletion is based on the given project_id and
    (optionally) the subject_id.

    Args:
        project_id (str): The project identifier. Corresponds to 'research_study_id' in the table.
        subject_id (str): The subject identifier. Corresponds to 'research_subject_id' in the table.
                           This parameter is optional. If not given, all subjects for the provided project_id will be deleted.

    Raises:
        Exception: An error occurred when executing the delete statement, other than "no such table" error.

    Returns:
        None
    """
    # Prepare the SQL delete statement for removing data related to the given project_id
    delete_stmt = f"""
                    delete
                    from 
                        research_subject
                    where research_study_id_type = 'xnat_project_id'
                    and research_study_id = '{project_id}'
                """
    
    # Add condition for subject_id to the delete statement if provided
    if subject_id:
        delete_stmt += f" and research_subject_id = '{subject_id}'"
    
    # Create a cursor for executing the delete statement
    cursor = engine.cursor()

    try:
        # Execute the delete statement
        cursor.execute(delete_stmt)
        # Commit changes to the database
        engine.commit()

    except Exception as error:
        error_string = str(error)
        # If the error is due to "no such table", silently ignore it. Otherwise, propagate the error.
        if "no such table" in error_string:
            None
        else:
            raise error



def get_existing_data(project_id: str, subject_id: str) -> pd.DataFrame:
    """
    Retrieves existing data from the 'research_subject' table based on the provided project_id and (optionally) subject_id.

    Args:
        project_id (str): The project identifier. Corresponds to 'research_study_id' in the table.
        subject_id (str): The subject identifier. Corresponds to 'research_subject_id' in the table.
                          This parameter is optional. If not provided, all subjects for the provided project_id will be fetched.

    Returns:
        pd.DataFrame: A DataFrame containing the existing data fetched from the 'research_subject' table.

    """
    # Prepare the SQL select statement to fetch data related to the given project_id
    query = f"""
                select * 
                from 
                    research_subject
                where research_study_id = '{project_id}'
                and research_study_id_type = 'xnat_project_id'
            """

    # Add condition for subject_id to the select statement if provided
    if subject_id:
        query += f" and research_subject_id = '{subject_id}'"

    # Execute the SQL statement and store the result in a DataFrame
    existing_df = pd.read_sql(query, engine)
    
    # Convert the datatypes of the DataFrame based on the table schema
    existing_df = convert_datatypes_based_on_table('research_subject', existing_df)

    # Return the DataFrame
    return existing_df



def set_study_attributes(df: pd.DataFrame, project_id: str) -> pd.DataFrame:
    """
    Replaces 'research_study_uri', 'research_study_title', and 'research_study_id_type' in the given DataFrame 
    with values fetched from the 'research_study' table using the provided project_id.

    Args:
        df (pd.DataFrame): The DataFrame containing research subjects.
        project_id (str): The project identifier used to fetch the study details.

    Returns:
        pd.DataFrame: The updated DataFrame with the new study attributes.
    """
    # Prepare the SQL statement to fetch research study details
    query = f"""
                select 
                    research_study_id,
                    research_study.research_study_uri,
                    research_study.research_study_title,
                    research_study.research_study_id_type
                from 
                    research_study
                where 
                    research_study.research_study_id = '{project_id}'
                    and research_study_id_type = 'xnat_project_id';
            """

    # Execute the SQL statement and store the result in a DataFrame
    lkp_df = pd.read_sql(query, engine)

    # Remove existing columns for study attributes from the original DataFrame
    df = df.drop(
        [
            'research_study_uri',
            'research_study_title',
            'research_study_id_type'
        ],
        axis=1, errors='ignore')

    # Merge the original DataFrame with the lookup DataFrame on the 'research_study_id'
    df = df.merge(
        lkp_df,
        on='research_study_id'
    )

    return df


def transform_subject(project_id: str, subject_list: list) -> pd.DataFrame:
    """
    Fetches the subject details from XNAT for each subject in the provided subject_list,
    transforms the data into a DataFrame, and sets study attributes.

    Args:
        project_id (str): The project identifier. 
        subject_list (list): List of subject identifiers to be fetched from XNAT.

    Returns:
        pd.DataFrame: A DataFrame containing the transformed subject data.
    """
    # Initialize an empty list to store subject details
    participant_list = []

    # Loop over each subject in the subject list
    for xnat_subject_id in subject_list:

        # Fetch the subject details from XNAT
        subject_resource = get_subject_data(project_id, xnat_subject_id)

        # Create a dictionary for each subject and append to the participant_list
        participant_list.append(
            research_subject(
                src_system='MPG XNAT',
                research_subject_id=subject_resource['xnat:Subject']['@label'],
                research_study_id=subject_resource['xnat:Subject']['@project'],
                xnat_custom_fields=json.dumps(json.loads(subject_resource['xnat:Subject'].get('xnat:custom_fields', '{}')))
            )
        )

    # Transform the list of dictionaries into a DataFrame
    subject_df = pd.DataFrame.from_dict(participant_list)

    # Replace empty strings in the DataFrame with None
    subject_df = subject_df.replace({"": None})

    if len(subject_df) > 0:
        # If the DataFrame is not empty, set the study project fields
        subject_df = set_study_attributes(subject_df, project_id)

        # Transform datatypes based on the 'research_subject' table schema
        subject_df = convert_datatypes_based_on_table('research_subject', subject_df)

    return subject_df



def main(project_id, subject_id, processing_date):

    if subject_id is None:
        subject_list = get_subject_list(project_id)
    else:
        subject_list = [subject_id]

    subject_df = transform_subject(project_id, subject_list)

    # get the existing data
    existing_df = get_existing_data(project_id, subject_id)

    delta_df = calculate_delta(
        existing_df, 
        subject_df, 
        'research_subject_uri', 
        pk_fields
    )

    load_df = parse_delta_results(
        nexus_base=config['nexus']['uri_base'],
        proc_dt=processing_date,
        uri_field_name='research_subject_uri',
        delta_df=delta_df,
        uri_salt_field_list = pk_fields
    )

    if len(load_df) > 0:

        load_df = convert_datatypes_based_on_table('research_subject', load_df)

        delete_existing_data(project_id, subject_id)

        load_df.to_sql(
            'research_subject',
            engine,
            if_exists='append',
            index=False
        )


if __name__ == '__main__':

    processing_date = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    project_id = sys.argv[1]

    try:
        subject_id = sys.argv[2]
    except:
        subject_id = None

    main(project_id, subject_id, processing_date)
