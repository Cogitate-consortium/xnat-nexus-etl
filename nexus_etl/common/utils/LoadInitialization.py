def get_env_variables():
    
    import os
    from DataEngineeringConfig import read_yaml_file
    
    # this script may be called by other ETL processes.
    # Need to change the working directory so that
    # the relative path's can be provided instead of absolute paths
    # and scripts can function correctly in any environment.
    
    caller_dir = os.getcwd()
    this_dir = os.path.dirname(__file__)
    os.chdir(this_dir)

    yaml_file_list = [
        str(os.path.abspath('../config/env_config.yaml')),
        str(os.path.abspath('../config/common_config.yaml'))
    ]

    config = read_yaml_file(yaml_file_list)
    
    # reset the working directory
    os.chdir(caller_dir)

    return config

