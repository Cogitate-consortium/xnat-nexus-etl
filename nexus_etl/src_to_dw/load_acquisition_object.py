import pandas as pd
import sys
import setup
from get_xnat_data import get_dicom_header, get_header
config = setup.config
import requests

from DbConnection import connect_to_db
from datetime import datetime
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table
from dw_dataclasses.acquisition import acquisition

engine = connect_to_db()
pk_fields = [
            'src_system',
            'research_subject_id',
            'research_study_id',
            'research_study_id_type',
            'session_id',
            'acquisition_id',
            'dummy_field'
        ]

def get_existing_data(project_id: str, subject_id: str, session_id: str) -> pd.DataFrame:

    # get the existing data
    query = f"""
                select * 
                from 
                    acquisition_object
                where research_study_id = '{project_id}'
                and research_study_id_type = 'xnat_project_id'
            """

    if subject_id:
        query += f" and research_subject_id = '{subject_id}'"

    if session_id:
        query += f" and session_id = '{session_id}'"
    
    existing_df = pd.read_sql(query, engine)

    return existing_df


def delete_existing_data(project_id: str):

    # get the existing data
    delete_stmt = f"""
                    delete
                    from 
                        acquisition_object
                    where research_study_id = '{project_id}'
                    and research_study_id_type = 'xnat_project_id'
                """
    
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

            
def main():

    # get the following from nifi param
    nifi_proc_dt = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    project_id = sys.argv[1]
    
    try:
        subject_id = sys.argv[2]
    except:
        subject_id = None
        
    try:
        session_id = sys.argv[3]
    except:
        session_id = None
        
    try:
        experiment_type = sys.argv[4]
    except:
        experiment_type = None



    # read in the list of acquisitions
    query = f"""
        select
            src_system,
            research_study_id,
            research_study_id_type,
            research_subject_id,
            session_id,
            session_type,
            acquisition_uri,
            acquisition_id,
            acquisition_type,
            acquisition_insert_date,
            acquisition_last_modified,
            acquisition_object_quality,
            device_manufacturer,
            device_name,
            session_date,
            research_study_uri,
            research_study_title,
            research_subject_uri,
            session_uri,
            device_uri,
            accession_id,
            acquisition_start_date,
            acquisition_start_time,
            series_description,
            xnat_custom_fields
        from 
            acquisition 
        where 
            research_study_id = '{project_id}'"""
    acquisition_object_df = pd.read_sql(query, engine)

    api_session = requests.Session()

    acquisition_object_df['dicom_header'] = None
    acquisition_object_df['non_dicom_header'] = None
    
    for index, acquisition in acquisition_object_df.iterrows():

        if acquisition['session_type'] in ['fif:megEegSessionData', 'edf:ecogSessionData', 'et:eyetrackerSessionData']:
            
            header_json = get_header(
                api_session, 
                project_id, 
                acquisition['research_subject_id'], 
                acquisition['session_id'], 
                acquisition['acquisition_id']
            )
            
            header_json = header_json['items'][0]['data_fields']
            
            filtered_header_json = {}
            
            for key in header_json:
                
                if 'parameters/bids_' in key:
                    
                    new_key = key.replace('parameters/bids_', 'bids:')
                    # remove the key from the dict
                    filtered_header_json[new_key] = header_json[key]

                if 'parameters/gnmd_' in key:
                    
                    new_key = key.replace('parameters/gnmd_', 'gnmd:')
                    # remove the key from the dict
                    filtered_header_json[new_key] = header_json[key]

            acquisition_object_df.at[index, 'non_dicom_header'] = filtered_header_json
            
        else:
            dicom_json = get_dicom_header(api_session, project_id, acquisition['session_id'], acquisition['acquisition_id'])
            dicom_header_json = dicom_json['ResultSet']['Result']
            acquisition_object_df.at[index, 'dicom_header'] = dicom_header_json
        

    acquisition_object_df = convert_datatypes_based_on_table('acquisition_object', acquisition_object_df)


    existing_df = get_existing_data(project_id, None, None)
    existing_df = convert_datatypes_based_on_table('acquisition_object', existing_df)

    existing_df['dummy_field'] = 'Acquisition_object_salt'
    acquisition_object_df['dummy_field'] = 'Acquisition_object_salt'

    delta_df = calculate_delta(
        existing_df, 
        acquisition_object_df, 
        'acquisition_object_uri', 
        pk_fields
    )

    load_df = parse_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = nifi_proc_dt, 
        uri_field_name = 'acquisition_object_uri', 
        delta_df = delta_df,
        uri_salt_field_list = pk_fields
    )

    if len(load_df) > 0:

        load_df = convert_datatypes_based_on_table('acquisition_object', load_df)

        delete_existing_data(project_id)

        # insert to postgres
        load_df.to_sql(
            'acquisition_object', 
            engine, 
            if_exists='append', 
            index=False
        )


if __name__ == "__main__":
    main()
