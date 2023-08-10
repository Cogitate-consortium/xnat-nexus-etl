import pandas as pd
import sys
import setup
import json
from datetime import datetime
from get_xnat_data import get_acquisition_data, get_session_datatypes
config = setup.config

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table

engine = connect_to_db()
pk_fields = [
            'src_system',
            'research_subject_id',
            'research_study_id',
            'research_study_id_type',
            'session_id',
            'acquisition_id'
        ]

def delete_existing_data(project_id: str, subject_id: str, session_id: str):

    # get the existing data
    delete_stmt = f"""
                    delete
                    from 
                        acquisition
                    where research_study_id = '{project_id}'
                    and research_study_id_type = 'xnat_project_id'
                """

    if subject_id:
        delete_stmt += f" and research_subject_id = '{subject_id}'"

    if session_id:
        delete_stmt += f" and session_id = '{session_id}'"

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


def get_existing_data(project_id: str, subject_id: str, session_id: str) -> pd.DataFrame:

    # get the existing data
    query = f"""
                select * 
                from 
                    acquisition
                where research_study_id = '{project_id}'
                and research_study_id_type = 'xnat_project_id'
            """

    if subject_id:
        query += f" and research_subject_id = '{subject_id}'"

    if session_id:
        query += f" and session_id = '{session_id}'"
    
    existing_df = pd.read_sql(query, engine)

    return existing_df


def set_study_attributes(df: pd.DataFrame, project_id: str) -> pd.DataFrame:
    
    # need to get the parent study uri and title
    query = f"""
                select 
                    research_study_id,
                    research_study.research_study_uri,
                    research_study.research_study_title
                from 
                    research_study
                where 
                    research_study.research_study_id = '{project_id}'
                    and research_study_id_type = 'xnat_project_id';
            """
    lkp_df = pd.read_sql(query, engine)

    df = df.drop(
        [
            'research_study_uri',
            'research_study_title'
        ],
        axis=1, errors='ignore')

    df = df.merge(
        lkp_df,
        on = 'research_study_id'
    )
    
    return df



def set_subject_attributes(df: pd.DataFrame, project_id: str, subject_id: str) -> pd.DataFrame:
    
    query = f"""
                select 
                    research_study_id,
                    research_subject_id, 
                    research_subject_uri
                from 
                    research_subject
                where 
                    research_study_id = '{project_id}'
                    and research_study_id_type = 'xnat_project_id'
            """
    
    if subject_id:
        query += f" and research_subject_id = '{subject_id}'"

    lkp_df = pd.read_sql(query, engine)

    df = df.drop(
        [
            'research_subject_uri'
        ],
        axis=1, errors='ignore')
            
    df = df.merge(
        lkp_df,
        on = ['research_study_id','research_subject_id']
    )
    
    return df



def set_session_attributes(df: pd.DataFrame, project_id: str, subject_id: str, session_id: str) -> pd.DataFrame:
    
    query = f"""
                select 
                    research_study_id,
                    research_subject_id, 
                    session_id,
                    session_uri
                from 
                    session
                where 
                    research_study_id = '{project_id}'
                    and research_study_id_type = 'xnat_project_id'
            """
    
    if subject_id:
        query += f" and research_subject_id = '{subject_id}'"
        
    if session_id:
        query += f" and session_id = '{session_id}'"
    
    lkp_df = pd.read_sql(query, engine)
    
    df = df.drop(
        [
            'session_uri'
        ],
        axis=1, errors='ignore')
    

    df = df.merge(
        lkp_df,
        on = ['research_study_id','research_subject_id','session_id']
    )
    
    return df



def set_device_attributes(df: pd.DataFrame) -> pd.DataFrame:
    
    query = f"""
                select 
                    device_uri,
                    device_manufacturer,
                    device_name
                from 
                    device
                where 
                    src_system = 'MPG XNAT'
            """
    
    lkp_df = pd.read_sql(query, engine)

    df = df.drop(
        [
            'device_uri'
        ],
        axis=1, errors='ignore')

    df = df.merge(
        lkp_df,
        on = ['device_manufacturer','device_name']
    )
    
    return df


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

    if not experiment_type:
        experiment_type_list = get_session_datatypes()
    else:
        experiment_type_list = [experiment_type]

        
    data_list = []
    for xnat_experiment_type in experiment_type_list:
        
        acquisition_list = get_acquisition_data(
            project_id=project_id, 
            xnat_experiment_type=xnat_experiment_type, 
            subject_id=subject_id, 
            experiment_id=session_id
        )

        for scan_data in acquisition_list:
            
            custom_fields = scan_data.get(f'{experiment_type}/custom_fields'.lower(), None)

            if custom_fields:
                custom_fields = json.loads(custom_fields)
            else:
                custom_fields = None
                
            scan_dict = {
                'src_system': "MPG XNAT",
                'research_study_id': project_id,
                'research_study_id_type': 'xnat_project_id',
                'research_subject_id': scan_data['subject_label'],
                'session_id': scan_data[f'{xnat_experiment_type}/label'.lower()],
                'session_type': xnat_experiment_type,
                'acquisition_id': scan_data['xnat:imagescandata/id'],
                'accession_id': scan_data['ID'],
                'acquisition_type': scan_data['xnat:imagescandata/type'],
                'acquisition_modality': scan_data['xnat:imagescandata/modality'],
                'acquisition_insert_date': scan_data['xnat:imagescandata/meta/insert_date'],
                'acquisition_last_modified': scan_data['xnat:imagescandata/meta/last_modified'],
                'acquisition_start_date': scan_data['xnat:imagescandata/start_date'],
                'acquisition_start_time': scan_data['xnat:imagescandata/starttime'],
                'acquisition_object_quality': scan_data['xnat:imagescandata/quality'],
                'device_manufacturer': scan_data['xnat:imagescandata/scanner/manufacturer'],
                'device_name': scan_data['xnat:imagescandata/scanner/model'],
                'series_description': scan_data['xnat:imagescandata/series_description'],
                'xnat_custom_fields': custom_fields
            }
            
            session_timestamp = None
            if scan_data[f'{xnat_experiment_type}/date'.lower()]:
                session_date = scan_data[f'{xnat_experiment_type}/date'.lower()]
                session_time = scan_data[f'{xnat_experiment_type}/time'.lower()]
                session_timestamp = f'{session_date} {session_time}'
            scan_dict['session_date'] = session_timestamp

            data_list.append(scan_dict)
        
    scan_df = pd.DataFrame.from_dict(data_list)
    scan_df = scan_df.replace({"": None})    
        

    if len(scan_df) > 0:
        scan_df = scan_df.replace({"": None})
        scan_df = set_study_attributes(scan_df, project_id)
        scan_df = set_subject_attributes(scan_df, project_id, subject_id)
        scan_df = set_session_attributes(scan_df, project_id, subject_id, session_id)
        scan_df = set_device_attributes(scan_df)
        scan_df = convert_datatypes_based_on_table('acquisition', scan_df)
        

    existing_df = get_existing_data(project_id, subject_id, session_id)
    existing_df = convert_datatypes_based_on_table('acquisition', existing_df)

    delta_df = calculate_delta(
        existing_df, 
        scan_df, 
        'acquisition_uri', 
        pk_fields
    )

    load_df = parse_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = nifi_proc_dt, 
        uri_field_name = 'acquisition_uri', 
        delta_df = delta_df,
        uri_salt_field_list = pk_fields
    )

    load_df = convert_datatypes_based_on_table('acquisition', load_df)

    if len(load_df) > 0:

        delete_existing_data(project_id, subject_id, session_id)

        # insert to postgres
        load_df.to_sql(
            'acquisition', 
            engine, 
            if_exists='append', 
            index=False
        )


if __name__ == "__main__":
    main()
