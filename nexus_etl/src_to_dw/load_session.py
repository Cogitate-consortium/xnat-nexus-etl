import pandas as pd
import sys
from get_xnat_data import get_session_data, get_session_datatypes
import setup
from datetime import datetime
import json
config = setup.config

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table
from dw_dataclasses.session import session

engine = connect_to_db()
pk_fields = [
            'src_system',
            'research_subject_id',
            'research_study_id',
            'research_study_id_type',
            'accession_id',
        ]

def delete_existing_data(project_id: str, subject_id: str, session_id: str):

    # get the existing data
    delete_stmt = f"""
                    delete
                    from 
                        session
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
                    session
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

    
    for experiment_type in experiment_type_list:
        
        session_list = get_session_data(project_id=project_id, xnat_experiment_type = experiment_type)

        for session_data in session_list:

            custom_fields = session_data.get(f'{experiment_type}/custom_fields'.lower(),'{}')
            if custom_fields:
                custom_fields = json.dumps(json.loads(custom_fields))
            else:
                custom_fields = {}
            
            session_dataclass = session(
                src_system='MPG XNAT',
                research_study_id=project_id,
                research_study_id_type='xnat_project_id',
                research_subject_id=session_data['subject_label'],
                session_id=session_data[f'{experiment_type}/label'.lower()],
                accession_id=session_data['ID'],
                session_type=experiment_type,
                xnat_custom_fields=custom_fields
            )

            session_timestamp = None
            if session_data[f'{experiment_type}/date'.lower()]:
                session_date = session_data[f'{experiment_type}/date'.lower()]
                session_time = session_data[f'{experiment_type}/time'.lower()]
                session_timestamp = f'{session_date} {session_time}'

            session_dataclass.session_date = session_timestamp
            data_list.append(session_dataclass)

    session_df = pd.DataFrame.from_dict(data_list)

    
    if len(session_df) > 0:
        session_df = set_study_attributes(session_df, project_id)
        session_df = set_subject_attributes(session_df, project_id, subject_id)
        session_df = convert_datatypes_based_on_table('session', session_df)
    
    existing_df = get_existing_data(project_id, subject_id, session_id)
    existing_df = convert_datatypes_based_on_table('session', existing_df)


    delta_df = calculate_delta(
        existing_df, 
        session_df, 
        'session_uri', 
        pk_fields
    )

    load_df = parse_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = nifi_proc_dt, 
        uri_field_name = 'session_uri', 
        delta_df = delta_df,
        uri_salt_field_list = pk_fields
    )

    load_df = convert_datatypes_based_on_table('session', load_df)

    if len(load_df) > 0:
        
        delete_existing_data(project_id, subject_id, session_id)

        # insert to postgres
        load_df.to_sql(
            'session', 
            engine, 
            if_exists='append', 
            index=False
        )
    

if __name__ == "__main__":
    main()