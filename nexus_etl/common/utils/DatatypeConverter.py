import pandas as pd
from DbConnection import connect_to_db

# create postgres connection
engine = connect_to_db()


def get_table_datatypes(table_name: str, non_default_engine=None) -> pd.DataFrame:
    """
    This function gets the schema information for a specified table in an SQLite database.

    Parameters:
    table_name (str): The name of the table from which to fetch the schema.
    non_default_engine (sqlalchemy.engine.base.Engine, optional): An SQLAlchemy engine object 
                                                                  pointing to an SQLite database.
                                                                  Defaults to None, in which case the 
                                                                  function uses a default engine.

    Returns:
    pd.DataFrame: A pandas DataFrame containing the schema information. The DataFrame includes 
                  information such as column id, column name, data type, whether the column can 
                  be NULL, and default value and primary key information.
    """

    # Prepare an SQLite PRAGMA statement to get table information
    query = f"""
        pragma table_info({table_name});
    """

    # If a non-default engine is specified, use it; otherwise, use the default engine
    if non_default_engine:
        df = pd.read_sql(sql=query, con=non_default_engine)
    else:
        df = pd.read_sql(sql=query, con=engine)

    # Return the DataFrame
    return df


def map_table_dtypes_to_pandas_dtypes(table_datatypes_df: pd.DataFrame, datatype_field_name='type', column_name_field_name='name') -> dict:
    """
    This function maps the data types of the columns in a database table to their corresponding pandas data types.
    
    Parameters:
    table_datatypes_df (pd.DataFrame): A DataFrame containing the schema information of a database table.
    datatype_field_name (str, optional): The field name in the DataFrame that contains the data types of the columns. 
                                         Defaults to 'type'.
    column_name_field_name (str, optional): The field name in the DataFrame that contains the names of the columns. 
                                            Defaults to 'name'.
    
    Returns:
    dict: A dictionary mapping column names to their corresponding pandas data types.
    """
    
    # Initialize an empty dictionary to store the mapping from column names to pandas data types
    pandas_dtype_dict = {}

    # Iterate over the rows in the DataFrame
    for index, row in table_datatypes_df.iterrows():

        # Map integer types to pandas Int64
        if row[datatype_field_name].lower() in ['integer']:
            pandas_dtype_dict[row[column_name_field_name]] = 'Int64'

        # Map text types to pandas str
        elif row[datatype_field_name].lower() in ['text','blob']:
            pandas_dtype_dict[row[column_name_field_name]] = 'str'

        # Map date and timestamp types to pandas datetime64[ns]
        elif row[datatype_field_name].lower() in ['datetime']:
            pandas_dtype_dict[row[column_name_field_name]] = 'datetime64[ns]'

        # Map boolean type and numeric flags to pandas bool
        elif (row[datatype_field_name].lower() in ['bool']) or (('flag' in row[column_name_field_name]) and (row[datatype_field_name].lower() in ['numeric'])):
            pandas_dtype_dict[row[column_name_field_name]] = 'bool'

        # Map float types and non-flag numeric types to pandas float64
        elif (row[datatype_field_name].lower() in ['float4','float8']) or (('flag' not in row[column_name_field_name]) and (row[datatype_field_name].lower() in ['numeric'])):
            pandas_dtype_dict[row[column_name_field_name]] = 'float64'

        # Raise an error if a data type is not mapped
        else:
            raise ValueError("Table datatype is not mapped to pandas datatype.")

    # Return the dictionary
    return pandas_dtype_dict


def convert_datatypes_ignorena(df: pd.DataFrame, column_name: str, target_datatype: str, float64_flag=False) -> pd.DataFrame:
    """
    This function converts the datatype of a column in a DataFrame to a specified type, while ignoring NA values.
    
    Parameters:
    df (pd.DataFrame): The DataFrame whose column's datatype is to be converted.
    column_name (str): The name of the column whose datatype is to be converted.
    target_datatype (str): The target datatype to which the column should be converted.
    float64_flag (bool, optional): A flag indicating whether to first convert the column to 'float64' before 
                                    converting to the target datatype. This is needed when converting a string with 
                                    a decimal point to int. Defaults to False.
    
    Returns:
    pd.DataFrame: The DataFrame with the column's datatype converted.
    """
    
    # If the target datatype is 'Int64'
    if target_datatype == 'Int64':
        # If float64_flag is set, first convert the column to 'float64'
        if float64_flag:
            df = df.astype({column_name:'float64'})
        # Then convert the column to 'Int64'
        df = df.astype({column_name:target_datatype})
    
    # If the target datatype is 'datetime64[ns]'
    elif target_datatype == 'datetime64[ns]':
        df = df.astype({column_name:target_datatype})

    # If the target datatype is not 'Int64' and the column contains any NA values
    elif (target_datatype != 'Int64') and (df[column_name].isnull().values.any()):
        # Split the DataFrame into non-null and null parts
        df_notnull = df.loc[df[column_name].notna()]
        df_null = df.loc[df[column_name].isna()]

        # If float64_flag is set, first convert the non-null part of the column to 'float64'
        if float64_flag:
            df_notnull = df_notnull.astype({column_name:'float64'})
        
        # Then convert the non-null part of the column to the target datatype
        df_notnull = df_notnull.astype({column_name:target_datatype})

        # Append the null part back to the DataFrame
        df = df_null.append(df_notnull)

    # If the target datatype is not 'Int64' and the column does not contain any NA values
    else:
        # If float64_flag is set, first convert the column to 'float64'
        if float64_flag:
            df = df.astype({column_name:'float64'})
        # Then convert the column to the target datatype
        df = df.astype({column_name:target_datatype})

    # Return the DataFrame with the column's datatype converted
    return df



def convert_datatypes_based_on_table(table_name, df, non_default_engine=None) -> pd.DataFrame:
    """
    This function converts the data types of the columns in a pandas DataFrame 
    to match the corresponding data types in a database table.
    
    Parameters:
    table_name (str): The name of the database table whose data types should be matched.
    df (pd.DataFrame): The DataFrame whose columns' data types are to be converted.
    non_default_engine (sqlalchemy.engine.base.Engine, optional): An SQLAlchemy engine object 
                                                                  pointing to an SQLite database.
                                                                  Defaults to None, in which case the 
                                                                  function uses a default engine.
    
    Returns:
    pd.DataFrame: The DataFrame with its columns' data types converted.
    """
    
    # Get the data types from the database
    database_dtype_df = get_table_datatypes(table_name, non_default_engine)

    # Create the data type dictionary
    dataframe_dtype_dict = map_table_dtypes_to_pandas_dtypes(database_dtype_df)

    # Convert the columns of the DataFrame
    for column in dataframe_dtype_dict:
        
        if column in df.columns:

            # If the column's data type is 'str' in both the DataFrame and the database
            if (df[column].dtype in ['str', 'object', 'O']) and dataframe_dtype_dict[column] == 'str':
                df = convert_datatypes_ignorena(df, column, dataframe_dtype_dict[column], False)
            
            # If the column's data type is 'str' in the DataFrame but 'Int64' in the database
            elif (df[column].dtype in ['str', 'O', 'object']) and dataframe_dtype_dict[column] == 'Int64':

                try:
                    df = convert_datatypes_ignorena(df, column, dataframe_dtype_dict[column], True)
                
                except Exception as error:
                    error_string = str(error)
                    # Ignore errors related to converting non-finite values to integer
                    if "Cannot convert non-finite values (NA or inf) to integer" not in error_string:
                        raise error
            
            # For all other data types
            else:
                df = convert_datatypes_ignorena(df, column, dataframe_dtype_dict[column])

    # Return the DataFrame with converted data types
    return df
