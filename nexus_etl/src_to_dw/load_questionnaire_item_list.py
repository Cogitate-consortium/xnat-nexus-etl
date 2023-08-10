import pandas as pd
import sys
from get_xnat_data import get_form_schema, get_session_datatypes
import setup
import numpy as np
import json
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
            'group_id',
            'question_id',
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
            

def set_questionnaire_attributes(df: pd.DataFrame, project_id: str, questionnaire_uuid: str) -> pd.DataFrame():

    # need to get the parent study uri and title
    query = f"""
                select 
                    research_study_id,
                    questionnaire_uri,
                    questionnaire_name,
                    questionnaire_title,
                    questionnaire_uuid,
                    subject_type,
                    xnat_data_type
                from 
                    questionnaire_list
                where 
                    research_study_id = '{project_id}'
                    and research_study_id_type = 'xnat_project_id'
                    and questionnaire_uuid = '{questionnaire_uuid}';
            """

    lkp_df = pd.read_sql(query, engine)

    df = df.drop(
        [
            'questionnaire_uri',
            'questionnaire_title',
            'questionnaire_name'
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
                    questionnaire_item_list
                where 
                    research_study_id = '{research_study_id}'
                    and research_study_id_type = 'xnat_project_id'
            """

    existing_df = pd.read_sql(query, engine)
    existing_df = convert_datatypes_based_on_table('questionnaire_item_list', existing_df) 
    
    return existing_df



def delete_existing_data(research_study_id: str) -> None:
    
    # get the existing data
    delete_stmt = f"""
                    delete
                    from 
                        questionnaire_item_list
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
These two functions work together to construct question dictionaries from form data. 
Each question is represented by a dictionary, and all questions are stored in a list. 

The construct_question_defn function handles the recursive nature of this process, 
allowing for questions that are nested within components or columns. 

The construct_component_details function handles the construction of individual question dictionaries.  
'''

def construct_component_details(project_id, group_id, item, question_dict_list, full_form_dict):
    """
    Constructs the details of a component from the form data.
    
    Args:
        project_id: The ID of the project.
        group_id: The ID of the group.
        item: The item in the form data.
        question_dict_list: The list of question dictionaries.
        full_form_dict: The dictionary representation of the form data.

    Returns:
        question_dict_list: The updated list of question dictionaries.
    """    
    if 'key' in item:  # Check if 'key' is present in the item.
        
        # Append a dictionary representing the item to the question dictionary list.
        question_dict_list.append(
            # key-value pairs for the item dictionary.
            {
                'research_study_id': project_id,
                'questionnaire_uuid': full_form_dict['components'][0]['key'],
                'group_id': group_id,
                'group_uri': None,
                'question_id': item['key'],
                'question_label': item['label'],
                'question_description': item.get('description', None),
                'question_type': item['type'],
                'required_flag': item.get('validate', {}).get('required', False),
                'enable_when_question': item.get('conditional', {}).get('when', None),
                'enable_when_operator': 'eq' if 'conditional' in item else None,
                'enable_when_answer': item.get('conditional', {}).get('eq', None),
                'validate': item.get('validate',{}),
                'repeats_flag': item.get('multiple',False)
            }
        )
    
    return question_dict_list


def construct_question_defn(project_id, group_id, xnat_question_struct, full_form_dict, question_dict_list, group_type = 'components'):
    """
    Constructs the definition of a question from the form data.
    
    Args:
        project_id: The ID of the project.
        group_id: The ID of the group.
        xnat_question_struct: The structure of the questions.
        full_form_dict: The dictionary representation of the form data.
        question_dict_list: The list of question dictionaries.
        group_type: The type of the group. Default is 'components'.

    Returns:
        question_dict_list: The updated list of question dictionaries.
    """    
    for item in xnat_question_struct[group_type]:
        
        # If 'components' is present in the item, construct the component details.
        if 'components' in item:
            
            # Recursive calls to construct_component_details and construct_question_defn
            # for nested structures.
            question_dict_list = construct_component_details(project_id, group_id, item, question_dict_list, full_form_dict)
            
            item_key = item.get('key', None)
            if item_key:
                if group_id:
                    new_group_id = f"{group_id}.{item['key']}"
                else:
                    new_group_id = item['key']
            else:
                new_group_id = group_id
                
                
            question_dict_list = construct_question_defn(
                project_id, 
                new_group_id,
                item,
                full_form_dict,
                question_dict_list,
                group_type = 'components'
            )
        
        # If 'columns' is present in the item, construct the component details.
        elif 'columns' in item:
            
            # Recursive calls to construct_component_details and construct_question_defn
            # for nested structures.
            question_dict_list = construct_component_details(project_id, group_id, item, question_dict_list, full_form_dict)
            
            item_key = item.get('key', None)
            if item_key:
                if group_id:
                    #new_group_id = f"{group_id}.{item['key']}"
                    new_group_id = group_id
                else:
                    #new_group_id = item['key']
                    new_group_id = None
            else:
                new_group_id = group_id
                
            question_dict_list = construct_question_defn(
                project_id, 
                new_group_id,
                item,
                full_form_dict,
                question_dict_list,
                group_type = 'columns'
            )
            
        else:  # If neither 'components' nor 'columns' is present in the item.
            question_dict_list = construct_component_details(project_id, group_id, item, question_dict_list, full_form_dict)
            
    return question_dict_list


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
                df = set_questionnaire_attributes(df, project_id, questionnaire_uuid)

                question_dict_list = construct_question_defn(
                    project_id,
                    None,
                    form_dict,
                    form_dict,
                    []
                )
                question_dict_list_df = pd.DataFrame.from_dict(question_dict_list)

                df = df.merge(
                        question_dict_list_df,
                        on=['research_study_id','questionnaire_uuid']
                    )

                full_df = full_df.append(df)


    # get existing data for comparison
    existing_df = get_existing_data(project_id)

    delta_df = calculate_delta(
        existing_df, 
        full_df, 
        'questionnaire_item_uri', 
        pk_fields
    )

    load_df = parse_delta_results(
        nexus_base = config['nexus']['uri_base'], 
        proc_dt = nifi_proc_dt, 
        uri_field_name = 'questionnaire_item_uri', 
        delta_df = delta_df,
        uri_salt_field_list = pk_fields
    )

    if len(load_df) > 0:


        group_uri_df = load_df.loc[
            load_df['question_type'].isin(['container','tree', 'datamap', 'datagrid', 'editgrid']),
            ['question_id', 'group_id', 'questionnaire_item_uri']
        ]

        if len(group_uri_df) > 0:
            for index, group_row in group_uri_df.iterrows():
                if group_row['group_id']:
                    group_uri_df.at[index, 'question_id'] = f"{group_row['group_id']}.{group_row['question_id']}"

        group_uri_df = group_uri_df.drop(columns='group_id')

        group_uri_df = group_uri_df.rename(
            columns={
                'questionnaire_item_uri':'group_uri',
                'question_id':'group_id'
            }
        )

        load_df = load_df.drop(columns='group_uri')

        load_df = load_df.merge(
            group_uri_df,
            on='group_id',
            how='left'
        ).replace({np.nan:None})

        delete_existing_data(project_id)

        load_df = convert_datatypes_based_on_table('questionnaire_item_list', load_df)

        # load to postgres
        load_df.to_sql(
            'questionnaire_item_list',
            engine,
            if_exists='append',
            index=False
        )


if __name__ == "__main__":
    main()
    