import pandas as pd
import sys
import setup
config = setup.config
import numpy as np
import json
from datetime import datetime

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_delta_results
from DatatypeConverter import convert_datatypes_based_on_table

engine = connect_to_db()
pk_fields = [
            'research_study_id',
            'research_study_id_type',
            'questionnaire_label',
            'response_subject_uri',
            'question_id',
            'response_code',
            'response_group_uri',
            'response_list_group_uri',
            'response_index_in_list'
        ]

from load_questionnaire_response_functions import recursive_creation_of_response_df, get_questionnaire_metadata, get_questionnaire_metadata_for_datamaps, create_first_iteration_of_response_df, parse_response_with_dict
from load_questionnaire_response_functions import assign_list_uri, get_existing_data, delete_existing_data, assign_group_uri

        
def main():

    project_id = sys.argv[1]
    nifi_proc_dt = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    resource_type = 'fhir:ResearchSubject'
    postgres_table = "research_subject"
    subject_id_field = 'research_subject_id'
    subject_uri_field = 'research_subject_uri'
    field_list = f"{subject_id_field}, xnat_custom_fields, {subject_uri_field}"


    # get the questionnaire metadata for this particular resource type
    schema_df = get_questionnaire_metadata(resource_type, project_id)
    datamap_schema_df = get_questionnaire_metadata_for_datamaps(resource_type, project_id)

    # get subject (the subject of the form) details
    query = f"""
            select
                {field_list}
            from
                {postgres_table}
                where research_study_id = '{project_id}'
                and xnat_custom_fields <> '{{}}'
            """

    subject_df = pd.read_sql(query, engine)


    questionnaire_response_df = pd.DataFrame()

    for index, subject in subject_df.iterrows():
                
        if subject['xnat_custom_fields']:
        
            response_dict = json.loads(subject['xnat_custom_fields'])
        
            df = create_first_iteration_of_response_df(response_dict)
                        
            df = recursive_creation_of_response_df(df, schema_df, datamap_schema_df, subject[subject_uri_field], subject[subject_id_field])
            
            questionnaire_response_df = questionnaire_response_df.append(df)
            

    # remove rows for questionnaires that aren't active anymore
    questionnaire_response_df = questionnaire_response_df.loc[~questionnaire_response_df['src_system'].isna()]


    # get the questionnaire response list details
    query = f"""
                select
                    ql.research_study_id,
                    ql.questionnaire_label,
                    ql.response_subject_uri,
                    ql.questionnaire_response_uri,
                    ql.subject_type,
                    ql.xnat_data_type
                from
                    questionnaire_response_list ql
                inner join {postgres_table} rs 
                    on ql.response_subject_uri = rs.{subject_uri_field}
            """
    questionnaire_response_metadata_df = pd.read_sql(query, engine)


    questionnaire_response_df = questionnaire_response_metadata_df.merge(
        questionnaire_response_df,
        on = ['research_study_id','questionnaire_label','response_subject_uri'],
        how = 'right'
    ).replace({np.nan:None})


    questionnaire_response_df.drop(
        questionnaire_response_df[
            (questionnaire_response_df.question_type=='xnatSelect')
            & (questionnaire_response_df.response_code_display != questionnaire_response_df.response_text)
        ].index, inplace=True)


    questionnaire_response_df = convert_datatypes_based_on_table(
        'questionnaire_response', 
        questionnaire_response_df
    ) 

    # replace empty values in response_text column with None
    #df['range'] = df['range'].str.replace(',','-')
    questionnaire_response_df = questionnaire_response_df.replace({'':None})
    questionnaire_response_df = questionnaire_response_df.replace({"['']":None})
    questionnaire_response_df = questionnaire_response_df.replace({"[]":None})

    # get existing data for comparison
    existing_df = get_existing_data(project_id, resource_type)

    # assign the group uri
    questionnaire_response_df = assign_group_uri(existing_df, questionnaire_response_df)

    # assign the list uri
    existing_df = existing_df.replace({None:"$"})
    questionnaire_response_df = questionnaire_response_df.replace({None:"$"})

    questionnaire_response_df = assign_list_uri(existing_df, questionnaire_response_df)

    existing_df = existing_df.replace({"$":None})
    questionnaire_response_df = questionnaire_response_df.replace({"$":None})

    # set the subject_response_type
    questionnaire_response_df['response_subject_type'] = resource_type

    # convert the datatype to make the delta comparison easier
    questionnaire_response_df = convert_datatypes_based_on_table(
        'questionnaire_response', 
        questionnaire_response_df
    ) 


    delta_df = calculate_delta(
        existing_df, 
        questionnaire_response_df, 
        'questionnaire_response_item_uri', 
        pk_fields
    )


    load_df = parse_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = nifi_proc_dt, 
        uri_field_name = 'questionnaire_response_item_uri', 
        delta_df = delta_df,
        uri_salt_field_list = pk_fields
    )


    if len(load_df) > 0:

        load_df = assign_group_uri(load_df, load_df)
        
        load_df = convert_datatypes_based_on_table('questionnaire_response', load_df)

        delete_existing_data(project_id, resource_type)

        # load to postgres
        load_df.to_sql(
            'questionnaire_response',
            engine,
            if_exists='append',
            index=False
        )



if __name__ == "__main__":
    main()
    
