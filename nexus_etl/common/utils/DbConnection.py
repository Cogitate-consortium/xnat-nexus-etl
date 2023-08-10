from sqlalchemy.engine.base import Engine
from LoadInitialization import get_env_variables
import sqlite3 as sl
import os

# Load environment variables
config = get_env_variables()

def connect_to_db() -> Engine:
    """
    This function establishes a connection to the SQLite database and returns an SQLAlchemy engine object.
    
    Returns:
    Engine: An SQLAlchemy engine object that connects to the SQLite database.
    """
        
    # Save the current working directory
    caller_dir = os.getcwd()

    # Get the directory of this script file
    this_dir = os.path.dirname(__file__)

    # Change the working directory to the directory of this script file
    os.chdir(this_dir)

    # Connect to the SQLite database located at the specified relative path
    con = sl.connect('../../database/mpg_eln.db')

    # Reset the working directory to its original state
    os.chdir(caller_dir)
        
    return con
