import pandas as pd
import sys
from get_xnat_data import get_form_schema, get_session_datatypes
import setup
from datetime import datetime
config = setup.config

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table

engine = connect_to_db()
pk_fields = [
            'research_study_id',
            'research_study_id_type',
            'questionnaire_name',
            'questionnaire_uuid',
            'xnat_data_type',
            'subject_type'
        ]

def set_study_attributes(df: pd.DataFrame, project_id: str) -> pd.DataFrame():

    # need to get the parent study uri and title
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

    lkp_df = pd.read_sql(query, engine)

    df = df.drop(
        [
            'research_study_uri',
            'research_study_title',
            'research_study_id_type'
        ],
        axis=1, errors='ignore')

    df = df.merge(
        lkp_df,
        on='research_study_id'
    )

    return df


def get_existing_data(research_study_id: str) -> None:
    
    # get the existing data
    query = f"""
                select * 
                from 
                    questionnaire_list
                where 
                    research_study_id = '{research_study_id}'
                    and research_study_id_type = 'xnat_project_id'
            """

    existing_df = pd.read_sql(query, engine)
    existing_df = convert_datatypes_based_on_table('questionnaire_list', existing_df) 
    
    return existing_df


def delete_existing_data(research_study_id: str) -> None:
    
    # get the existing data
    delete_stmt = f"""
                    delete
                    from 
                        questionnaire_list
                    where 
                        research_study_id = '{research_study_id}'
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
    
    project_id = sys.argv[1]
    nifi_proc_dt = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    form_resource_type_list = [
        'xnat:projectData',
        'xnat:subjectData'
    ]

    session_type_list = get_session_datatypes()

    form_resource_type_mapping = {
        'xnat:projectData':'fhir:ResearchStudy',
        'xnat:subjectData':'fhir:ResearchSubject'
    }

    for resource_type in session_type_list:
        form_resource_type_mapping[resource_type] = 'nidm:Session'
    
    form_resource_type_list = form_resource_type_list + session_type_list

    df_list = []

    for form_resource_type in form_resource_type_list:
        
        form_dict_list = get_form_schema(form_resource_type, project_id)
        
        if form_dict_list:
            for form_dict in form_dict_list['components']:

                data_dict = {
                    'src_system': 'MPG XNAT',
                    'research_study_id': project_id,
                    "questionnaire_uuid": form_dict['components'][0]['key'],
                    'questionnaire_name': form_dict['title'],
                    'questionnaire_title': form_dict['title'],
                    'xnat_data_type': form_resource_type,
                    'subject_type': form_resource_type_mapping[form_resource_type]
                }

                df_list.append(data_dict)

    df = pd.DataFrame.from_dict(df_list)
    df = set_study_attributes(df,project_id)
    
    # get existing data for comparison
    existing_df = get_existing_data(project_id)

    delta_df = calculate_delta(
        existing_df, 
        df, 
        'questionnaire_uri', 
        pk_fields
    )

    load_df = parse_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = nifi_proc_dt, 
        uri_field_name = 'questionnaire_uri', 
        delta_df = delta_df,
        uri_salt_field_list = pk_fields
    )

    if len(load_df) > 0:

        load_df = convert_datatypes_based_on_table('questionnaire_list', load_df)

        delete_existing_data(project_id)

        # load to postgres
        load_df.to_sql(
            'questionnaire_list',
            engine,
            if_exists='append',
            index=False
        )


if __name__ == "__main__":
    main()
    