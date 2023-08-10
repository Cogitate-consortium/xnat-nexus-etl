import numpy as np
import pandas as pd
import uuid
import datetime
from DatatypeConverter import convert_datatypes_ignorena
from NexusUriGenerator import generate_uri, generate_uri_column


def hash_pk_fields(df: pd.DataFrame, duplicate_columns_to_consider: list) -> pd.DataFrame:
    # This function receives a dataframe and a list of columns to consider for hashing. 
    # It returns the original dataframe with an additional column "hash". This column 
    # contains a unique hash value generated from the values of the columns 
    # specified in "duplicate_columns_to_consider".

    # If the length of dataframe is greater than 0, i.e., if the dataframe is not empty.
    if len(df) > 0:
        # A new column 'hash' is created in the dataframe. Each row's hash value is calculated 
        # by applying the Python built-in 'hash' function to a tuple of the values from the columns 
        # specified in "duplicate_columns_to_consider". 
        # The 'apply' function applies a function along an axis of the DataFrame, in this case, each row (axis=1).
        # The 'lambda' function takes a row of data ('x'), converts it into a tuple and then 
        # converts it into a string to pass to the 'hash' function.
        df['hash'] = df[duplicate_columns_to_consider].apply(lambda x: hash(str(tuple(x))), axis = 1)
    else:
        # If the dataframe is empty, the 'hash' column is filled with 'None'.
        df['hash'] = None
    
    # The function returns the original dataframe with the added 'hash' column.
    return df



def calculate_delta(existing_df, new_df, uri_field_name, pk_fields) -> pd.DataFrame:
    # This function is used to calculate the delta, or changes, between two dataframes, 
    # existing_df and new_df, based on the specified primary key fields (pk_fields). It returns a new
    # dataframe that shows what rows have been inserted, updated, or deleted.
    
    # If the existing_df is empty, then all rows in new_df are considered 'INSERT'.
    if len(existing_df) == 0:
        df_join = new_df
        df_join['delta_action'] = 'INSERT'
        
    # If the existing_df is not empty, then the function proceeds with the following steps.
    elif len(existing_df) > 0:
        # Replacing np.nan values with None in both dataframes to avoid issues due to datatype mismatch
        existing_df = existing_df.replace({np.nan: None})
        new_df = new_df.replace({np.nan: None})

        # Specifying certain columns to consider for duplicate checking and non primary key columns
        nexus_fields = [uri_field_name, '_createdat', '_updatedat', '_rev']
        duplicate_columns_to_consider = list(set(list(existing_df.columns)) - set(nexus_fields))
        non_pk_fields = set(list(existing_df.columns)) - set(pk_fields) - set(nexus_fields)

        # Hashing primary key fields to check for duplicates
        existing_df = hash_pk_fields(existing_df, duplicate_columns_to_consider)
        new_df = hash_pk_fields(new_df, duplicate_columns_to_consider)

        # Merging both dataframes into one
        data_dict = {"Prev": existing_df,"Curr": new_df}
        df_merged = pd.concat(data_dict, sort = True)
        
        # Dropping duplicates based on the 'hash' value and also keeping track of existing unchanged rows
        df_dedup = df_merged.drop_duplicates(subset='hash', keep=False)
        df_existing = df_merged[df_merged.duplicated(subset='hash', keep=False)]
        df_dedup = df_dedup.drop(columns=['hash'])
        df_existing = df_existing.drop(columns=['hash'])

        # For existing unchanged rows, marking them as 'NOCHANGE'
        if len(df_existing) > 0:
            df_existing = df_existing[df_existing['_rev'].notna()]
            df_existing['delta_action'] = 'NOCHANGE'

        # Checking the remaining rows for insertions, updates and deletions
        df_join = pd.DataFrame()
        if len(df_dedup) > 0:
            # Separating previous and current deduplicated dataframes
            try:
                df_dedup_curr = df_dedup.loc["Curr"]
            except:
                df_dedup_curr = pd.DataFrame(columns = df_dedup.columns)

            try:
                df_dedup_prev = df_dedup.loc["Prev"]
            except:
                df_dedup_prev = pd.DataFrame(columns = df_dedup.columns)

            # Dropping unnecessary columns
            df_dedup_prev = df_dedup_prev.drop(list(non_pk_fields), axis=1)
            df_dedup_curr = df_dedup_curr.drop(nexus_fields, axis=1)

            # Merging previous and current dataframes to find insertions, updates and deletions
            df_join = df_dedup_curr.merge(df_dedup_prev, on=list(pk_fields),
                                how='outer', indicator=True)
            df_join.insert(loc = len(df_join.columns),
                            column = 'delta_action',
                            value = None)

            # Labeling rows based on their status: UPDATE, INSERT or DELETE
            df_join.loc[df_join['_merge'] == 'both', 'delta_action'] = 'UPDATE'
            df_join.loc[df_join['_merge'] == 'left_only', 'delta_action'] = 'INSERT'
            df_join.loc[df_join['_merge'] == 'right_only', 'delta_action'] = 'DELETE'
            df_join = df_join.drop('_merge', axis=1)

        # Appending the existing unchanged rows to the joined dataframe
        df_join = df_join.append(df_existing)

    # If there are any duplicate rows in the dataframe after all operations, raise a ValueError
    if df_join.duplicated(subset=pk_fields).any():
        print(pd.concat(g for _, g in df_join.groupby(pk_fields) if len(g) > 1))
        raise ValueError("Duplicates found in dataframe after delta check.  Dataframe will not be inserted.")

    # Returning the final dataframe
    return df_join



def parse_delta_results(nexus_base: str, proc_dt: str, uri_field_name: str, delta_df: pd.DataFrame, generate_uri_flag: bool = True, uri_salt_field_list: list = []) -> pd.DataFrame:
    """
    The function parses the delta results and prepares the dataframe for loading. It handles the processing of 
    new insertions, updates, and unchanged data based on the 'delta_action' column in the provided delta_df.
    It also converts data types of specific columns and generates URIs for new records if needed.

    Args:
    nexus_base (str): The base string used for generating URIs.
    proc_dt (str): The processing datetime, it's used to update '_createdat' and '_updatedat' fields.
    uri_field_name (str): The field name used for the URI.
    delta_df (pd.DataFrame): The input DataFrame containing delta results.
    generate_uri_flag (bool, optional): If True, generates URIs for new records. Default is True.
    uri_salt_field_list (list, optional): The list of fields used for generating the URI. Default is an empty list.

    Returns:
    pd.DataFrame: The output DataFrame ready for loading.
    """

    load_df = pd.DataFrame() # create an empty dataframe for loading

    # Only process if there are records in the delta_df
    if len(delta_df) > 0:
    
        # Convert _rev column to Int64 if present in dataframe
        if '_rev' in delta_df.columns:
            delta_df = delta_df.astype({'_rev':'Int64'})
        
        # Splitting the delta_df into separate dataframes based on delta_action type
        nochange_df = delta_df.loc[delta_df['delta_action'] == 'NOCHANGE']
        update_df = delta_df.loc[delta_df['delta_action'] == 'UPDATE']
        insert_df = delta_df.loc[delta_df['delta_action'] == 'INSERT']
        delete_df = delta_df.loc[delta_df['delta_action'] == 'DELETE']

        proc_dt = datetime.datetime.strptime(proc_dt, '%Y-%m-%d %H:%M:%S') # Convert proc_dt to a datetime object

        # Processing update records - increment _rev and set _updatedat to current processing date
        if len(update_df) > 0:
            update_df['_updatedat'] = proc_dt
            update_df['_rev'] += 1

        # Processing insert records - set _createdat, _updatedat, and _rev. Generate URIs if needed
        if len(insert_df) > 0:
            insert_df['_createdat'] = proc_dt
            insert_df['_updatedat'] = proc_dt
            insert_df['_rev'] = 1

            # Generate URIs for new records if the flag is set
            if generate_uri_flag:
                if uri_salt_field_list: # Generate URIs using the uri_salt_field_list if it's not empty
                    insert_df = generate_uri_column(insert_df, uri_field_name, nexus_base, uri_salt_field_list)
                else: # Generate URIs using a uuid if the uri_salt_field_list is empty
                    insert_df[uri_field_name] = [f"{nexus_base}{uuid.uuid4()}" for _ in range(len(insert_df.index))]

        # Combine nochange, insert, and update dataframes to form the final load dataframe
        load_df = nochange_df.append(insert_df)
        load_df = load_df.append(update_df)

        # Drop the delta_action column
        load_df = load_df.drop(columns=['delta_action'])

        # Convert the data types of specific columns
        load_df = convert_datatypes_ignorena(load_df, '_rev', 'int32')
        load_df = convert_datatypes_ignorena(load_df, '_createdat', 'datetime64[ns]')
        load_df = convert_datatypes_ignorena(load_df, '_updatedat', 'datetime64[ns]')

    return load_df



def parse_questionnaire_response_delta_results(nexus_base: str, proc_dt: str, uri_field_name: str, delta_df: pd.DataFrame, generate_uri: bool = True, uri_salt_field_list: list = []) -> pd.DataFrame:
    """
    The function parses the delta results from questionnaire responses and prepares the dataframe for loading.
    It handles the processing of new insertions, updates, and unchanged data based on the 'delta_action' column 
    in the provided delta_df. It also converts data types of specific columns and generates URIs for new records
    if needed. It differentiates between records with answer options and those without.

    Args:
    nexus_base (str): The base string used for generating URIs.
    proc_dt (str): The processing datetime, it's used to update '_createdat' and '_updatedat' fields.
    uri_field_name (str): The field name used for the URI.
    delta_df (pd.DataFrame): The input DataFrame containing delta results.
    generate_uri (bool, optional): If True, generates URIs for new records. Default is True.
    uri_salt_field_list (list, optional): The list of fields used for generating the URI. Default is an empty list.

    Returns:
    pd.DataFrame: The output DataFrame ready for loading.
    """

    load_df = pd.DataFrame() # create an empty dataframe for loading

    # Only process if there are records in the delta_df
    if len(delta_df) > 0:
    
        # Convert _rev column to numeric if present in dataframe
        if '_rev' in delta_df.columns:
            delta_df["_rev"] = pd.to_numeric(delta_df["_rev"])
        
        # Splitting the delta_df into separate dataframes based on delta_action type
        nochange_df = delta_df.loc[delta_df['delta_action'] == 'NOCHANGE']
        update_df = delta_df.loc[delta_df['delta_action'] == 'UPDATE']
        insert_df = delta_df.loc[delta_df['delta_action'] == 'INSERT']
        delete_df = delta_df.loc[delta_df['delta_action'] == 'DELETE']
        
        proc_dt = datetime.datetime.strptime(proc_dt, '%Y-%m-%d %H:%M:%S') # Convert proc_dt to a datetime object

        # Processing update records - increment _rev and set _updatedat to current processing date
        if len(update_df) > 0:
            update_df['_updatedat'] = proc_dt
            update_df['_rev'] += 1

        # Processing insert records - handle differently for records with answer options and those without
        if len(insert_df) > 0:
            insert_df['_createdat'] = proc_dt
            insert_df['_updatedat'] = proc_dt
            insert_df['_rev'] = 1
            
            # Split the insert dataframe into two subsets: one with answer options and one without
            insert_option_subset_df = insert_df.loc[insert_df['question_type'].isin(['select','xnatSelect','radio','selectboxes'])]
            insert_nonoption_subset_df = insert_df.loc[~insert_df['question_type'].isin(['select','xnatSelect','radio','selectboxes'])]

            # Generate URIs for new records with answer options if the flag is set
            if generate_uri:
                if uri_salt_field_list: # Generate URIs using the uri_salt_field_list if it's not empty
                    insert_option_subset_df = generate_uri_column(insert_option_subset_df, uri_field_name, nexus_base, uri_salt_field_list)
                else: # Generate URIs using UUID if the uri_salt_field_list is empty
                    insert_option_subset_df[uri_field_name] = [f"{nexus_base}{uuid.uuid4()}" for _ in range(len(insert_option_subset_df.index))]

            # Set the URI field to None for records without answer options
            insert_nonoption_subset_df[uri_field_name] = None

            # Combine the two subsets back into insert_df
            insert_df = insert_option_subset_df.append(insert_nonoption_subset_df)

        # Append the different dataframes into the load dataframe
        load_df = nochange_df.append(insert_df)
        load_df = load_df.append(update_df)

        # Drop the delta_action column as it's not needed for loading
        load_df = load_df.drop(columns=['delta_action'])

        # Perform datatype conversion
        load_df = convert_datatypes_ignorena(load_df, '_rev', 'int32')
        load_df = convert_datatypes_ignorena(load_df, '_createdat', 'datetime64[ns]')
        load_df = convert_datatypes_ignorena(load_df, '_updatedat', 'datetime64[ns]', False)

    return load_df



def parse_questionnaire_delta_results(nexus_base: str, proc_dt: str, uri_field_name: str, delta_df: pd.DataFrame, uri_salt_field_list: list = []) -> pd.DataFrame:
    """
    The function parses the delta results from questionnaire data and prepares the dataframe for loading.
    It handles the processing of new insertions, updates, and unchanged data based on the 'delta_action' column 
    in the provided delta_df. It also converts data types of specific columns and generates URIs for new records
    if needed. It differentiates between records with answer options and those without.

    Args:
    nexus_base (str): The base string used for generating URIs.
    proc_dt (str): The processing datetime, it's used to update '_createdat' and '_updatedat' fields.
    uri_field_name (str): The field name used for the URI.
    delta_df (pd.DataFrame): The input DataFrame containing delta results.
    uri_salt_field_list (list, optional): The list of fields used for generating the URI. Default is an empty list.

    Returns:
    pd.DataFrame: The output DataFrame ready for loading.
    """

    load_df = pd.DataFrame() # create an empty dataframe for loading

    # Only process if there are records in the delta_df
    if len(delta_df) > 0:
    
        # Convert _rev column to numeric if present in dataframe
        if '_rev' in delta_df.columns:
            delta_df["_rev"] = pd.to_numeric(delta_df["_rev"])
        
        # Splitting the delta_df into separate dataframes based on delta_action type
        nochange_df = delta_df.loc[delta_df['delta_action'] == 'NOCHANGE']
        update_df = delta_df.loc[delta_df['delta_action'] == 'UPDATE']
        insert_df = delta_df.loc[delta_df['delta_action'] == 'INSERT']
        delete_df = delta_df.loc[delta_df['delta_action'] == 'DELETE']
        
        proc_dt = datetime.datetime.strptime(proc_dt, '%Y-%m-%d %H:%M:%S') # Convert proc_dt to a datetime object

        # Processing update records - increment _rev and set _updatedat to current processing date
        if len(update_df) > 0:
            update_df['_updatedat'] = proc_dt
            update_df['_rev'] += 1

        # Processing insert records - handle differently for records with answer options and those without
        if len(insert_df) > 0:
            insert_df['_createdat'] = proc_dt
            insert_df['_updatedat'] = proc_dt
            insert_df['_rev'] = 1
            
            # Split the insert dataframe into two subsets: one with answer options and one without
            insert_option_subset_df = insert_df.loc[insert_df['question_type'].isin(['select','xnatSelect','radio','selectboxes'])]
            insert_nonoption_subset_df = insert_df.loc[~insert_df['question_type'].isin(['select','xnatSelect','radio','selectboxes'])]
            
            # Generate URIs for new records with answer options if the uri_salt_field_list is not empty
            if len(uri_salt_field_list) > 0:
                insert_option_subset_df = generate_uri_column(insert_option_subset_df, uri_field_name, nexus_base, uri_salt_field_list)

            else:
                insert_option_subset_df[uri_field_name] = [f"{nexus_base}{uuid.uuid4()}" for _ in range(len(insert_option_subset_df.index))]

            # Set the URI field to None for records without answer options
            insert_nonoption_subset_df[uri_field_name] = None

            # Combine the two subsets back into insert_df
            insert_df = insert_option_subset_df.append(insert_nonoption_subset_df)

        # Append the different dataframes into the load dataframe
        load_df = nochange_df.append(insert_df)
        load_df = load_df.append(update_df)

        # Drop the delta_action column as it's not needed for loading
        load_df = load_df.drop(columns=['delta_action'])

        # Perform datatype conversion
        load_df = convert_datatypes_ignorena(load_df, '_rev', 'int32')
        load_df = convert_datatypes_ignorena(load_df, '_createdat', 'datetime64[ns]')
        load_df = convert_datatypes_ignorena(load_df, '_updatedat', 'datetime64[ns]')

    return load_df
