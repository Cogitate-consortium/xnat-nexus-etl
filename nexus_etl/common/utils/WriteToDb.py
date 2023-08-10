import numpy as np
import pandas as pd
import sys
from io import StringIO
import logging

def replace_special_chars(df: pd.DataFrame, column_list_to_replace: list) -> pd.DataFrame:
    """
    Replace special characters in a DataFrame.

    This function replaces any backslashes (\\) in the specified columns with two backslashes (\\\\).

    Args:
    df (pd.DataFrame): The input DataFrame.
    column_list_to_replace (list): The list of columns in which to replace special characters.

    Returns:
    pd.DataFrame: The DataFrame with special characters replaced.
    """

    # Check if DataFrame is not empty
    if len(df) > 0:
        for column_name in column_list_to_replace:
            if column_name in df.columns:
                if not df[column_name].isnull().all():
                    df = df.replace({column_name:{r'\\': r'\\\\'}}, regex=True) # replace backslashes

    return df

def replace_whitespace_chars(df: pd.DataFrame, column_list_to_replace: list) -> pd.DataFrame:
    """
    Replace whitespace characters in a DataFrame.

    This function replaces any newlines and carriage returns in the specified columns with the string '\\r'.

    Args:
    df (pd.DataFrame): The input DataFrame.
    column_list_to_replace (list): The list of columns in which to replace whitespace characters.

    Returns:
    pd.DataFrame: The DataFrame with whitespace characters replaced.
    """

    # Check if DataFrame is not empty
    if len(df) > 0:
        for column_name in column_list_to_replace:
            if column_name in df.columns:
                if not df[column_name].isnull().all():
                    df = df.replace({column_name:{r'\r\n': r'\\r'}}, regex=True) # replace newlines and carriage returns
                    df = df.replace({column_name:{r'\r': r'\\r'}}, regex=True)
                    df = df.replace({column_name:{r'\n': r'\\r'}}, regex=True)

    return df