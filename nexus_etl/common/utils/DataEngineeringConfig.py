import yaml
import os
import glob

def read_yaml_file(yaml_file_list: list) -> dict:
    '''
    Reads in YAML files containing project parameters and returns a Python dictionary.

    The YAML files can contain tags (reference to other parameters) such as !join and !replace which are replaced by this function
    with the content of what that tag is referencing.

            Parameters:
                    yaml_file_list (list): list of yaml file paths

            Returns:
                    Python dictionary of YAML file content with tags appropriately handled.
    '''
    yaml_file = ''

    for names in yaml_file_list:

        # Open each file in read mode
        with open(names,"r") as fp:
            temp_file = fp.read()
        yaml_file+=temp_file
        yaml_file+='\n'
    
    def join(loader, node):
        seq = loader.construct_sequence(node)
        return ''.join([str(i) for i in seq])


    def replace(loader, node):
        seq = loader.construct_sequence(node)
        return seq[2].replace(seq[1], seq[0])

    ## register the tag handler
    yaml.add_constructor('!join', join)
    yaml.add_constructor('!replace', replace)

    ## using your sample data
    return yaml.load(yaml_file, Loader=yaml.Loader)
    

    
        