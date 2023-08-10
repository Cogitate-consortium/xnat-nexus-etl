import pandas as pd
import sys
from get_xnat_data import get_form_schema, get_session_datatypes
import setup
from datetime import datetime
config = setup.config

from DbConnection import connect_to_db
from DeltaCalcUtils import calculate_delta, parse_questionnaire_delta_results
from DatatypeConverter import convert_datatypes_based_on_table

engine = connect_to_db()
pk_fields = [
            'research_study_id',
            'research_study_id_type',
            'questionnaire_uuid',
            'group_id',
            'question_id',
            'answer_option_code',
            'subject_type',
            'xnat_data_type'
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


def set_questionnaire_item_attributes(df: pd.DataFrame, project_id: str, questionnaire_uuid: str) -> pd.DataFrame():

    # need to get the parent study uri and title
    query = f"""
                select 
                    research_study_id,
                    questionnaire_uuid,
                    questionnaire_name,
                    questionnaire_title,
                    questionnaire_uri,
                    subject_type,
                    xnat_data_type,
                    group_id,
                    group_uri,
                    question_id,
                    question_label,
                    question_type,
                    question_description,
                    questionnaire_item_uri,
                    required_flag
                from 
                    questionnaire_item_list
                where 
                    research_study_id = '{project_id}'
                    and research_study_id_type = 'xnat_project_id'
                    and questionnaire_uuid = '{questionnaire_uuid}';
            """

    lkp_df = pd.read_sql(query, engine)

    df = df.drop(
        [
            'questionnaire_title',
            'questionnaire_name',
            'questionnaire_uri',
            'subject_type',
            'xnat_data_type',
            'group_id',
            'group_uri'
            'question_id',
            'question_label',
            'question_type',
            'question_description',
            'questionnaire_item_uri',
            'required_flag'
        ],
        axis=1, errors='ignore')

    df = df.merge(
        lkp_df,
        on=['research_study_id','questionnaire_uuid']
    )

    return df


def get_existing_data(research_study_id: str) -> None:
    
    # get the existing data
    query = f"""
                select * 
                from 
                    questionnaire
                where 
                    research_study_id = '{research_study_id}'
                    and research_study_id_type = 'xnat_project_id'
            """

    existing_df = pd.read_sql(query, engine)
    existing_df = convert_datatypes_based_on_table('questionnaire', existing_df) 
    
    return existing_df


def delete_existing_data(research_study_id: str) -> None:
    
    # get the existing data
    delete_stmt = f"""
                    delete
                    from 
                        questionnaire
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
            

'''
These two functions work together to construct option dictionaries from form data. 
Each option is represented by a dictionary, and all options are stored in a list. 

The construct_option_defn function handles the recursive nature of this process, 
allowing for options that are nested within components or columns. 

The construct_option_details function handles the construction of 
individual option dictionaries.
'''
def construct_option_details(project_id, item, option_dict_list):
    """
    Constructs the details of an option from the form data.
    
    Args:
        project_id: The ID of the project.
        item: The item in the form data.
        option_dict_list: The list of option dictionaries.

    Returns:
        option_dict_list: The updated list of option dictionaries.
    """
    if 'key' in item:  # Check if 'key' is present in the item.
        # 'data' or 'values' fields contain option list for a question.
        if 'data' or 'values' in item:    
            option_list = []
            if 'data' in item:
                # option_list gets values if 'data' field has 'values' field
                option_list = item['data'].get('values',[])
            elif 'values' in item:
                option_list = item['values']
            
            # loop over option list to append option dict to option_dict_list
            for option_item in option_list:
                option_dict = {
                    'question_id': item['key'],
                    'answer_option_code': option_item['label'],
                    'answer_option_display': option_item['value']
                }
                option_dict_list.append(option_dict)
        else:
            option_dict = {
                'question_id': item['key'],
                'answer_option_code': None,
                'answer_option_display': None
            }
            option_dict_list.append(option_dict)
    
    return option_dict_list

def construct_option_defn(project_id, xnat_question_struct, option_dict_list, group_type = 'components'):
    """
    Constructs the definition of an option from the form data.
    
    Args:
        project_id: The ID of the project.
        xnat_question_struct: The structure of the questions.
        option_dict_list: The list of option dictionaries.
        group_type: The type of the group. Default is 'components'.

    Returns:
        option_dict_list: The updated list of option dictionaries.
    """
    for item in xnat_question_struct[group_type]:  # Iterate through each item in the question structure.
        
        # If 'components' is present in the item, construct the option details.
        if 'components' in item:
            
            # Recursive calls to construct_option_details and construct_option_defn for nested structures.
            option_dict_list = construct_option_details(project_id, item, option_dict_list)
                
            option_dict_list = construct_option_defn(
                project_id, 
                item,
                option_dict_list,
                group_type = 'components'
            )
        
        # If 'columns' is present in the item, construct the option details.
        elif 'columns' in item:
            
            # Recursive calls to construct_option_details and construct_option_defn for nested structures.
            option_dict_list = construct_option_details(project_id, item, option_dict_list)
            
            option_dict_list = construct_option_defn(
                project_id, 
                item,
                option_dict_list,
                group_type = 'columns'
            )
            
        else:  # If neither 'components' nor 'columns' is present in the item.
            option_dict_list = construct_option_details(project_id, item, option_dict_list)
            
    return option_dict_list


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

    full_df = pd.DataFrame()

    for form_resource_type in form_resource_type_list:
        
        form_dict_list = get_form_schema(form_resource_type, project_id)
        
        if form_dict_list:

            for form_dict in form_dict_list['components']:

                questionnaire_uuid = form_dict['components'][0]['key']

                data_dict = {
                    'src_system': 'MPG XNAT',
                    'research_study_id': project_id,
                    'questionnaire_uuid': questionnaire_uuid
                }

                df = pd.DataFrame.from_dict([data_dict])

                df = set_study_attributes(df,project_id)
                df = set_questionnaire_item_attributes(df, project_id, questionnaire_uuid)

                # now parse the items from the json
                option_dict_list = []
                option_dict_list = construct_option_defn(
                    project_id,
                    form_dict,
                    []
                )

                if len(option_dict_list) > 0:
                    option_dict_list_df = pd.DataFrame.from_dict(option_dict_list)
                else:
                    option_dict_list_df = pd.DataFrame(columns=['question_id','answer_option_code','answer_option_display'])

                df = df.merge(
                        option_dict_list_df,
                        on=['question_id'],
                        how='left'
                    )

                full_df = full_df.append(df)
        
    # get existing data for comparison
    existing_df = get_existing_data(project_id)

    delta_df = calculate_delta(
        existing_df, 
        full_df, 
        'questionnaire_item_answer_option_uri', 
        pk_fields
    )

    load_df = parse_questionnaire_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = nifi_proc_dt, 
        uri_field_name = 'questionnaire_item_answer_option_uri', 
        delta_df = delta_df,
        uri_salt_field_list = pk_fields
    )


    if len(load_df) > 0:

        load_df = convert_datatypes_based_on_table('questionnaire', load_df)

        delete_existing_data(project_id)

        # load to postgres
        load_df.to_sql(
            'questionnaire',
            engine,
            if_exists='append',
            index=False
        )


if __name__ == "__main__":
    main()
    
