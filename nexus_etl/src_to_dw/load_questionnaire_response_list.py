import pandas as pd
import sys
import setup
from datetime import datetime

config = setup.config
from get_xnat_data import get_session_datatypes

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table

engine = connect_to_db()
pk_fields = [
            'research_study_id',
            'research_study_id_type',
            'questionnaire_uuid',
            'response_subject_uri'
        ]

def delete_existing_data(resource_type: str, research_study_id: str) -> None:
    
    # get the existing data
    delete_stmt = f"""
                    delete
                    from 
                        questionnaire_response_list
                    where 
                        research_study_id = '{research_study_id}'
                        and research_study_id_type = 'xnat_project_id'
                """

    cursor = engine.cursor()
    cursor.execute(delete_stmt)
    engine.commit()

    try:
        cursor.execute(delete_stmt)
        engine.commit()

    except Exception as error:
        error_string = str(error)
        if "no such table" in error_string:
            None
        else:
            raise error
            

def get_existing_data(resource_type: str, research_study_id: str) -> None:
    
    # get the existing data
    query = f"""
                select * 
                from 
                    questionnaire_response_list
                where 
                    research_study_id = '{research_study_id}'
                    and research_study_id_type = 'xnat_project_id'
                    and response_subject_type = '{resource_type}'
            """

    existing_df = pd.read_sql(query, engine)
    existing_df = convert_datatypes_based_on_table('questionnaire_response_list', existing_df) 
    
    return existing_df


def main():

    project_id = sys.argv[1]
    nifi_proc_dt = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    resource_type_list = [
        {
            'xnat_data_type': 'xnat:projectData',
            'resource_type': 'fhir:ResearchStudy',
            'postgres_table': "research_study",
            'subject_id_field': 'research_study_id',
            'subject_uri_field': 'research_study_uri'
        },
        {
            'xnat_data_type': 'xnat:subjectData',
            'resource_type': 'fhir:ResearchSubject',
            'postgres_table': "research_subject",
            'subject_id_field': 'research_subject_id',
            'subject_uri_field': 'research_subject_uri'
        }
    ]


    session_type_list = get_session_datatypes()

    questionnaire_response_list = []

    for resource_type in session_type_list:
            
            resource_type_list.append(
                {
                    'xnat_data_type': resource_type,
                    'resource_type': 'nidm:Session',
                    'postgres_table': "session",
                    'subject_id_field': 'session_id',
                    'subject_uri_field': 'session_uri'
                }
            )

    for resource_type_dict in resource_type_list:

        resource_type = resource_type_dict['resource_type']
        postgres_table = resource_type_dict['postgres_table']
        subject_id_field = resource_type_dict['subject_id_field']
        subject_uri_field = resource_type_dict['subject_uri_field']
        xnat_data_type = resource_type_dict['xnat_data_type']
        field_list = f"{subject_id_field}, {subject_uri_field}"


        # get subject (the subject of the form) details
        query = f"""
                select
                    {field_list}
                from
                    "{postgres_table}"
                    where research_study_id = '{project_id}'
                """

        subject_df = pd.read_sql(query, engine)


        response_list = []

        # we are going to initialize a response for each questionnaire that exists for this resource type
        query = f"""
                    select
                        ql.*
                    from 
                        questionnaire_list ql 
                    where 
                        ql.subject_type = '{resource_type}'
                        and xnat_data_type = '{xnat_data_type}'
                        and research_study_id = '{project_id}'
                """

        questionnaires_df = pd.read_sql(query, engine)


        for index0, subject_row in subject_df.iterrows():

            for index, row in questionnaires_df.iterrows():

                response_list.append(
                    {
                        "src_system": row['src_system'],
                        "research_study_id": row['research_study_id'],
                        "research_study_id_type": row['research_study_id_type'],
                        "research_study_uri": row['research_study_uri'],
                        "research_study_title": row['research_study_title'],
                        "questionnaire_uri": row['questionnaire_uri'],
                        "questionnaire_uuid": row['questionnaire_uuid'],
                        "questionnaire_label": row['questionnaire_name'],
                        'subject_type': row['subject_type'],
                        'xnat_data_type': row['xnat_data_type'],
                        "response_subject_uri": subject_row[subject_uri_field],
                        "response_subject_id": subject_row[subject_id_field],
                        "response_subject_type": resource_type
                    }
                )

        df = pd.DataFrame.from_dict(response_list)

        questionnaire_response_list.append(df)


    df = pd.concat(questionnaire_response_list)


    # get existing data for comparison
    existing_df = get_existing_data(resource_type, project_id)

    delta_df = calculate_delta(
        existing_df, 
        df,
        'questionnaire_response_uri', 
        pk_fields
    )


    load_df = parse_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = nifi_proc_dt, 
        uri_field_name = 'questionnaire_response_uri', 
        delta_df = delta_df,
        uri_salt_field_list = pk_fields
    )


    if len(load_df) > 0:

        load_df = convert_datatypes_based_on_table('questionnaire_response_list', load_df)

        delete_existing_data(resource_type, project_id)

        # load to postgres
        load_df.to_sql(
            'questionnaire_response_list',
            engine,
            if_exists='append',
            index=False
        )


if __name__ == "__main__":
    main()
    
