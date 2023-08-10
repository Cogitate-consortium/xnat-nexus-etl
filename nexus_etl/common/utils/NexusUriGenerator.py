import uuid
import random as rd
import pandas as pd
import numpy as np
rnd = rd.Random()
from LoadInitialization import get_env_variables

# Load the environment variables
config = get_env_variables()

# Extract the URI salt from the environment variables
uri_salt = config['nexus']['uri_salt']
uri_salt_delimiter = config['nexus']['uri_salt_delimiter']

def generate_uri_column(df: pd.DataFrame, uri_field_name: str, nexus_uri_base: str, uri_salt_field_list: list) -> pd.DataFrame:
    """
    Generate a URI for each row in the DataFrame based on the specified fields, a salt delimiter and an additional random salt string.

    Args:
    df (pd.DataFrame): The input DataFrame.
    uri_field_name (str): The name of the new URI column to add to the DataFrame.
    nexus_uri_base (str): The base string used for generating URIs.
    uri_salt_field_list (list): The list of fields in the DataFrame used for generating the URI.

    Returns:
    pd.DataFrame: The DataFrame with the new URI column added.
    """

    # Add the random salt to the list of fields used for generating the URI
    uri_salt_field_list.append('_salt')

    # Create a new column in the DataFrame for the URI. The URI is generated using the fields in uri_salt_field_list and a random salt.
    if len(df) > 0:
        df[uri_field_name] = df.assign(_salt=uri_salt).apply(lambda x: generate_uri(nexus_uri_base, uri_salt_delimiter.join(x[uri_salt_field_list].fillna('').astype('str'))), axis=1)

        uri_salt_field_list.remove('_salt')
    
    else:
        df[uri_field_name] = None

    return df

def generate_uri(nexus_uri_base: str, seed_value: str) -> str:
    """
    Generate a random URI using a base string and a seed value.

    Args:
    nexus_uri_base (str): The base string used for generating the URI.
    seed_value (str): The seed value used for generating the random part of the URI.

    Returns:
    str: The generated URI.
    """

    # Seed the random number generator with the seed value
    rnd.seed(seed_value)

    # Generate a random UUID
    random_uuid = uuid.UUID(int=rnd.getrandbits(128), version=4)

    # Combine the base string and the random UUID to create the URI
    random_uri = f"{nexus_uri_base}{str(random_uuid)}"

    return random_uri
