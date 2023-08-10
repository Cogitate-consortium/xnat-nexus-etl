'''
The following code provides functions responsible for parsing and handling data related to 
questionnaires from a database, specifically an XNAT-based system. To simplify the understanding, 
let's break down the purpose of each function:

create_first_iteration_of_response_df: 
Creates a DataFrame from a dictionary. The DataFrame has 
columns for the key, value, and an index for the key.

parse_response_with_dict: 
This function processes each row of the DataFrame 
created by create_first_iteration_of_response_df. If the 'value' of the row 
is a dictionary, it flattens it and appends it to a temporary DataFrame.

recursive_creation_of_response_df: 
This function is used to recursively 
process the DataFrame until no dictionaries or lists are present in the 
'value' column. It also merges the processed DataFrame with a schema DataFrame 
and a datamap_schema DataFrame based on some conditions.

get_existing_data: 
This function retrieves existing questionnaire response 
data from a database for a given research study id and response subject type.

delete_existing_data: 
This function deletes existing questionnaire response 
data from a database for a given research study id and response subject type.

assign_group_uri: 
This function is used to assign a group URI to the 
DataFrame's rows based on existing data.

fetch_list_uri: 
This function is used to retrieve a list group URI from 
the existing DataFrame. It's used by assign_list_uri.

assign_list_uri: 
This function assigns list URIs to rows of the DataFrame 
based on the existing data.
'''
import pandas as pd
import setup
config = setup.config
import numpy as np
import uuid

from DbConnection import connect_to_db
from DatatypeConverter import convert_datatypes_based_on_table
from NexusUriGenerator import generate_uri

engine = connect_to_db()

def get_questionnaire_metadata(subject_type, project_id):
    
    # get schema details
    query = f"""
        select
            q.answer_option_code response_code,
            q.answer_option_display response_code_display,
            q.group_id question_group_id,
            q.group_uri question_group_uri,
            q.question_id,
            q.question_label,
            q.question_type,
            q.questionnaire_item_answer_option_uri response_code_uri,
            q.questionnaire_item_uri,
            --q.questionnaire_name,
            q.questionnaire_uuid,
            q.questionnaire_title questionnaire_label,
            q.questionnaire_uri,
            q.required_flag,
            q.research_study_id,
            q.research_study_id_type,
            q.research_study_title,
            q.research_study_uri,
            q.src_system
        from
            questionnaire q
        inner join questionnaire_list ql 
        on
            q.questionnaire_uri = ql.questionnaire_uri
        and ql.subject_type = '{subject_type}'
        and ql.research_study_id = '{project_id}'
    """

    df = pd.read_sql(query, engine)
    
    return df


def get_questionnaire_metadata_for_datamaps(subject_type, project_id):
    
    query = f"""
            select
            q.answer_option_code response_code,
            q.answer_option_display response_code_display,
            q.group_id || '.' || q.question_id question_group_id,
            q.questionnaire_item_uri question_group_uri,
            q.questionnaire_uuid,
            q.questionnaire_title questionnaire_label,
            q.questionnaire_uri,
            q.research_study_id,
            q.research_study_id_type,
            q.research_study_title,
            q.research_study_uri,
            q.src_system
        from
            questionnaire q
        inner join questionnaire_list ql 
                on q.questionnaire_uri = ql.questionnaire_uri
        where 
            question_type = 'datamap'
            and ql.subject_type = '{subject_type}'
            and ql.research_study_id = '{project_id}'
    """
    
    df = pd.read_sql(query, engine)
    
    return df


def create_first_iteration_of_response_df(response_dict):
    """
    Function to create the first version of the DataFrame from a response dictionary.
    
    Parameters:
        response_dict (dict): A dictionary with responses.
        
    Returns:
        df (DataFrame): A DataFrame containing key value pairs from the response_dict.
    """
    df = pd.DataFrame()
    key_index = 0
    for k in response_dict:
        df = df.append(
            pd.DataFrame.from_dict([{
                'variable': k,
                'value': response_dict[k],
                'response_index_in_list': key_index,
                'question_group_id': None,
                'response_list_group_uri': None
            }])
        )

        key_index+=1
    df.reset_index(inplace=True, drop=True)
    
    return df


def parse_response_with_dict(row):
    """
    Function to parse a row with a dictionary as a value.
    
    Parameters:
        row (Series): A row of the DataFrame.
        
    Returns:
        temp_df (DataFrame): A DataFrame after parsing the row.
    """
    temp_df = pd.DataFrame()

    group_uri = generate_uri(
        nexus_uri_base = config['nexus']['uri_base'],
        seed_value = f"{np.add.reduce(row[['question_group_id','variable','response_index_in_list']].astype('str').replace({None:''}))}"
        )
    
    key_count = 0
    
    existing_group_id = None
    if row['question_group_id']:
        existing_group_id = f"{row['question_group_id']}."
    else:
        # If it's null, it's likely the root container for the survey.
        # The variable field will contain the UUID of the survey which should
        # be unique.
        existing_group_id = ""

    # Iterating over the dictionary in 'value'

    for k in row['value']:
        
        # Check for different conditions of the values and handle accordingly
        if row['value'][k] == False:
            
            # ignore this response.  It is for a checkbox-type question and the respondent selected false.
            True
            
        elif row['value'][k] == True:
            # Handle multiselect values
            temp_df = temp_df.append(
                pd.DataFrame.from_dict(
                    [{
                        'variable': row['variable'], 
                        'value': k, 
                        'response_index_in_list': f"{row['response_index_in_list']}.{key_count}", 
                        'question_group_id': existing_group_id.rstrip('.'),
                        'response_list_group_uri': group_uri,
                        'multiselect': True
                    }]
                )
            )
            
        else:
            
            temp_df = temp_df.append(
                pd.DataFrame.from_dict(
                    [{
                        'variable': k, 
                        'value': row['value'][k], 
                        'response_index_in_list': f"{row['response_index_in_list']}.{key_count}", 
                        'question_group_id': f"{existing_group_id}{row['variable']}",
                        'response_list_group_uri': group_uri,
                        'multiselect': False
                    }]
                )
            )

        key_count+=1
    
    return temp_df


def recursive_creation_of_response_df(df, schema_df, datamap_schema_df, subject_uri, subject_id):

    #count_of_rows_without_dictlist = 0
    contains_dict_or_list = 1
    while_loop_count = 0

    while contains_dict_or_list > 0:

        temp_df = pd.DataFrame()

        for index, row in df.iterrows():

            if isinstance(row['value'], dict):
                
                temp_df = temp_df.append(parse_response_with_dict(row))
                df.at[index, 'value'] = None

            elif isinstance(row['value'], list):

                list_count = 0
                for item in row['value']:
                    group_uri = generate_uri(
                                nexus_uri_base = config['nexus']['uri_base'],
                                seed_value = f"{np.add.reduce(row[['question_group_id','variable','response_index_in_list']].astype('str').replace({None:''}))}{list_count}"
                                )

                    if '.' in str(row['response_index_in_list']):
                        response_index_in_list = row['response_index_in_list'].rsplit('.',1)[0]
                    else:
                        response_index_in_list = row['response_index_in_list']

                    temp_df = temp_df.append(
                        pd.DataFrame.from_dict([
                            {
                                'variable': row['variable'],
                                'value':item,
                                'response_index_in_list': f"{response_index_in_list}.{list_count}",
                                'question_group_id': row['question_group_id'],
                                'response_list_group_uri': group_uri
                            }
                        ]))
                    list_count+=1

                df = df.drop([index])
                    
        df = df.append(temp_df)
        df.reset_index(inplace=True, drop=True)
        
        contains_dict_or_list = 0
        for index, row in df.iterrows():    
            if (isinstance(row['value'], dict)) or (isinstance(row['value'], list)):
                contains_dict_or_list=+1
                    

    df = df.rename(columns={
        'variable': 'question_id',
        'value': 'response_text'
    })

    df_original_columns = df.columns

    # add the multiselect column if it doesn't exist and set to false
    if 'multiselect' not in df.columns:
        df['multiselect'] = False


    df_nonmultiselect = df.loc[df['multiselect']==False]
    df_schema_nonmultiselect = schema_df.loc[~schema_df['question_type'].isin(['selectboxes'])]
    
    df_schema_multiselect = schema_df.loc[schema_df['question_type'].isin(['selectboxes'])]
    df_multiselect = df.loc[df['multiselect']==True]
    
    df_nonmultiselect = df_schema_nonmultiselect.merge(
        df_nonmultiselect,
        on=['question_id','question_group_id'],
        how='right'
    )

    df_multiselect = df_schema_multiselect.merge(
        df_multiselect,
        right_on = ['question_id','question_group_id', 'response_text'],
        left_on = ['question_id','question_group_id', 'response_code_display'],
        how='inner'
    )
    
    df = df_nonmultiselect.append(df_multiselect)
    
    # remove questions for option-type questions where that option wasn't selected
    df = df.loc[~((df['question_type'].isin(['select'])) & (df['response_code_display'] != df['response_text']))]
    
    
    # there are some questions in XNAT like a datamap that allow users to enter custom keys and values
    # these keys are to be treated as questions but will not be found in questionnaire schema definition
    # therefore, get the questionnaire details that the datamap is a part of.  Question details won't be found.
    missing_metadata_df = df.loc[(df['src_system'].isna()) & (~df['question_group_id'].isna())].copy()

    df = df.loc[(~df['src_system'].isna())].copy()

    # revert dataframe to the original columns to avoid duplicates when we join
    missing_metadata_df = missing_metadata_df[df_original_columns]

    missing_metadata_df = datamap_schema_df.merge(
        missing_metadata_df,
        on = 'question_group_id',
        how = 'right'
    )

    df = df.append(missing_metadata_df)

    df['response_subject_uri'] = subject_uri
    df['response_subject_id'] = subject_id

    df = df.drop(columns=['multiselect'])
        
    return df


def get_existing_data(research_study_id: str, response_subject_type: str) -> None:
    
    # get the existing data
    query = f"""
                select * 
                from 
                    questionnaire_response
                where 
                    research_study_id = '{research_study_id}'
                    and research_study_id_type = 'xnat_project_id'
                    and response_subject_type = '{response_subject_type}'
            """

    existing_df = pd.read_sql(query, engine)
    existing_df = convert_datatypes_based_on_table('questionnaire_response', existing_df) 
    
    return existing_df


def delete_existing_data(research_study_id: str, response_subject_type: str) -> None:
    
    # get the existing data
    delete_stmt = f"""
                    delete
                    from 
                        questionnaire_response
                    where 
                        research_study_id = '{research_study_id}'
                        and research_study_id_type = 'xnat_project_id'
                        and response_subject_type = '{response_subject_type}'
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
            

def assign_group_uri(existing_df, df):

    group_uri_df = existing_df.loc[
        existing_df['question_type'].isin(['columns','container','tree', 'datagrid', 'editgrid', 'datamap']),
        ['question_id', 'question_group_id', 'questionnaire_response_item_uri', 'response_index_in_list']
    ].drop_duplicates(keep='first')

    if len(existing_df) > 0:
        for index, group_row in group_uri_df.iterrows():
            if group_row['question_group_id']:
                group_uri_df.at[index, 'question_id'] = f"{group_row['question_group_id']}.{group_row['question_id']}"

    group_uri_df = group_uri_df.drop(columns='question_group_id')

    group_uri_df = group_uri_df.rename(
        columns={
            'questionnaire_response_item_uri':'response_group_uri',
            'question_id':'question_group_id',
            'response_index_in_list': 'group_index'
        }
    )
    
    
    if 'response_group_uri' in df.columns:
        df = df.drop(columns='response_group_uri')
    
    # also need to compute the group id in the current data
    df['group_index'] = None
    
    for index, row in df.iterrows():
        if row['response_index_in_list']:
            if '.' in row['response_index_in_list']:
                group_index = row['response_index_in_list'].rsplit('.',1)[0]
                df.at[index, 'group_index'] = group_index
            else:
                # this question might be linked to the parent group or questionnaire and doesn't have a group index
                group_index = None
                df.at[index, 'group_index'] = group_index

    df = df.merge(
        group_uri_df,
        on=['question_group_id','group_index'],
        how='left'
    ).replace({np.nan:None})
    
    df = df.drop(columns='group_index')

    return df


def fetch_list_uri(existing_df, row, include_list_index = False):
    
    response_list_group_uri = None
    
    if include_list_index:
        response_list_group_uri = existing_df.loc[
            (existing_df['response_subject_uri'] == row['response_subject_uri'])
            & (existing_df['questionnaire_label'] == row['questionnaire_label'])
            & (existing_df['question_id'] == row['question_id'])
            & (existing_df['question_group_id'] == row['question_group_id'])
            & (existing_df['response_group_uri'] == row['response_group_uri']) 
            & (existing_df['response_index_in_list'] == row['response_index_in_list'])
            ,'response_list_group_uri'
        ]
    else:
        response_list_group_uri = existing_df.loc[
            (existing_df['response_subject_uri'] == row['response_subject_uri']) 
            & (existing_df['questionnaire_label'] == row['questionnaire_label']) 
            & (existing_df['question_id'] == row['question_id']) 
            & (existing_df['question_group_id'] == row['question_group_id']) 
            & (existing_df['response_group_uri'] == row['response_group_uri'])
            ,'response_list_group_uri'
        ]

    if (len(response_list_group_uri) > 1) and (include_list_index == False):
        response_list_group_uri = fetch_list_uri(existing_df, row, True)
        
    elif len(response_list_group_uri) > 1:
        raise ValueError("Multiple URI's found.")
        
    else:
        None
        
    return response_list_group_uri


def assign_list_uri(existing_df, df):

    # first assign the list uris that already exist
    for index, row in df.iterrows():
        
        # find the list group uri in the existing df if it exists;  if it doesn't leave the newly generated one as is
        if row['response_index_in_list']:
            response_list_group_uri = fetch_list_uri(existing_df, row, include_list_index=True)
        else:
            response_list_group_uri = fetch_list_uri(existing_df, row)
                        
        if len(response_list_group_uri) == 1:
            df.at[index, 'response_list_group_uri'] = response_list_group_uri.values[0]

        else:
            True
                
    return df


