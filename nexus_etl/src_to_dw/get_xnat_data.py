import os
import json
import xmltodict, json
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import setup
import html

config = setup.config

from DbConnection import connect_to_db

# define postgres connection
datamart_engine = connect_to_db()

basedir = os.path.join(os.path.dirname(__file__), '..')

from pyxnat import Interface
interface = Interface(config=os.path.join(basedir, 'sensitive/xnat_config.cfg'))
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_project_data(xnat_project_id: str) -> dict:
    """
    Description:
    Get the project metadata from XNAT.  Fetches the XML project metadata and parses it into a Python dictionary.
    
    Keyword Arguments:
    xnat_project_id -- the XNAT project ID
    
    Returns:
    The dictionary representation of the project metadata.
    """
    
    # get the project metadata
    project_resource = interface.select.project(xnat_project_id).get()
    project_resource = xmltodict.parse(project_resource)

    return project_resource
    

def get_subject_list(xnat_project_id: str) -> list:
    
    # get subject metadata from XNAT
    subject_list = interface.select.project(xnat_project_id).subjects().get()
    
    return subject_list


def get_subject_data(xnat_project_id: str, xnat_subject_id: str) -> dict:
        
    subject_resource = interface.select.project(xnat_project_id).subject(xnat_subject_id).get()
    subject_resource = xmltodict.parse(subject_resource)

    return subject_resource

    
def get_session_data(project_id: str, xnat_experiment_type: str, subject_id: str=None, experiment_id: str=None) -> list:
    
    # get session data
    session_data_list = []

    session_data_list = interface.array.experiments(
        project_id = project_id,
        subject_label = subject_id,
        experiment_label = experiment_id,
        experiment_type=xnat_experiment_type,
        columns=[
            'xnat:subjectData/label',
            f'{xnat_experiment_type}/LABEL',
            f'{xnat_experiment_type}/DATE',
            f'{xnat_experiment_type}/TIME',
            f'{xnat_experiment_type}/custom_fields'
        ]
    ).data

    return session_data_list


def get_session_data_all(project_id: str, xnat_experiment_type: str, subject_id: str=None, experiment_id: str=None) -> list:
    
    # get session data
    session_data_list = []

    session_data_list = interface.array.experiments(
        project_id = project_id,
        subject_label = subject_id,
        experiment_label = experiment_id
    ).data

    return session_data_list


def get_acquisition_data(project_id: str, xnat_experiment_type: str, subject_id: str=None, experiment_id: str=None) -> list:

    # construct the acquisition resource
    scan_data_list = []

    scan_data_list.extend(
        interface.array.scans(
            project_id = project_id,
            subject_label = subject_id,
            experiment_label = None,
            experiment_type=xnat_experiment_type,
            columns=[
                'xnat:subjectData/label',
                'xnat:imageScanData/type',
                'xnat:imageScanData/meta/insert_date', 
                'xnat:imageScanData/meta/last_modified',
                'xnat:imageScanData/quality',
                'xnat:imageScanData/modality',
                'xnat:imageScanData/scanner/model',
                'xnat:imageScanData/scanner/manufacturer',
                f'{xnat_experiment_type}/DATE',
                f'{xnat_experiment_type}/TIME',
                f'{xnat_experiment_type}/LABEL',
                'xnat:imageScanData/series_description',
                'xnat:imageScanData/starttime',
                'xnat:imageScanData/start_date'
            ]
        ).data
    )
    
    return scan_data_list

def get_dicom_header(api_session, project_id: str, experiment_id: str, scan_id: str) -> list:
    
    r = api_session.get(f"{setup.xnat_server}/REST/services/dicomdump?src=/archive/projects/{project_id}/experiments/{experiment_id}/scans/{scan_id}&format=json&requested_screen=DicomScanTable.vm",
        headers = {
            "accept": "application/json;charset=UTF-8",
        },
        auth=(setup.xnat_username, setup.xnat_password),
        verify=False
    )
    
    if r.status_code == 200:
        
        #print(r.text)
        #result = json.loads(html.unescape(r.text))
        result = json.loads(r.text)
        return result
    else:
        print(r.status_code)
        raise ValueError("Could not fetch DICOM header information.")
        

def get_header(api_session, project_id: str, subject_id: str, experiment_id: str, scan_id: str) -> list:
    
    r = api_session.get(
        f"{setup.xnat_server}/data/projects/{project_id}/subjects/{subject_id}/experiments/{experiment_id}/scans/{scan_id}?format=json",
        headers = {
            "accept": "*/*",
        },
        auth=(setup.xnat_username, setup.xnat_password),
        verify=False
    )
    
    if r.status_code == 200:
        
        result = json.loads(html.unescape(r.text))
        return result
    
    else:
        print(r.status_code)
        raise ValueError(f"""
            Could not fetch DICOM header information.
            
            Error Information: 
            
            {r.text}
            
        """)
    
            
def get_form_schema(datatype: str, project_id: str) -> dict:

    '''
        datatype in xnat:projectData
        project_id = XNAT projectId
    '''
    
    url = f'{setup.xnat_server}/xapi/customforms/element'
    headers = {
        "accept": "application/json;charset=UTF-8",
    }
    auth=(setup.xnat_username, setup.xnat_password)
    
    params = {
        'xsiType': datatype,
        'projectId': project_id,
        'id': project_id,
        'appendPrevNextButtons': 'false'
    }
    
    r = requests.request('GET', url, headers=headers, params=params, auth=auth, verify=False) 

    if r.status_code == 200:
        form_dict = json.loads(r.text)
        return form_dict
    else:
        raise ValueError("Could not fetch form schema.")



def get_session_datatypes() -> list:
    
    '''
        Returns a list of 'Sesison' data types configured in XNAT.  Session data types
        have been identified by any data type with the keyword 'Session' in it.
        
        It also removes the imageSessionData data type which seems to include all the sub-types
        already being returned.  Therefore, it's been removed to avoid having duplicates.
    '''
    
    url = f'{setup.xnat_server}/xapi/schemas/datatypes'
    headers = {
        "accept": "application/json;charset=UTF-8",
    }
    auth=(setup.xnat_username, setup.xnat_password)

    params = {
        'appendPrevNextButtons': 'false'
    }

    r = requests.request('GET', url, headers=headers, params=params, auth=auth, verify=False) 
    
    xnat_datatype_list = json.loads(r.text)
    xnat_session_list = []

    for item in xnat_datatype_list:
        if 'Session' in item:
            xnat_session_list.append(item)
    
    xnat_session_list.remove('xnat:imageSessionData')
    
    return xnat_session_list