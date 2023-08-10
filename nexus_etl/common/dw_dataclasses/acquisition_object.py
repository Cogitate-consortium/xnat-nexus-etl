from dataclasses import dataclass
from dw_dataclasses.base import Base
import datetime
import json
import ast

@dataclass
class acquisition_object(Base):
    """acquisition_object"""
    src_system: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_study_uri: str=None
    research_study_title: str=None
    research_subject_id: str=None
    research_subject_uri: str=None
    session_id: str=None
    session_type: str=None
    session_date: datetime.datetime=None
    session_uri: str=None    
    acquisition_id: str=None
    acquisition_type: str=None
    #acquisition_modality: str=None
    acquisition_insert_date: datetime.datetime=None
    acquisition_last_modified: datetime.datetime=None
    acquisition_uri: str=None
    device_manufacturer: str=None
    device_name: str=None
    device_uri: str=None
    acquisition_object_quality: str=None
    acquisition_object_uri: str=None
    dicom_header: object=None
    non_dicom_header: object=None
    xnat_custom_fields: object=None
    accession_id: str=None
    acquisition_start_date: str=None
    acquisition_start_time: str=None
    series_description: str=None
    dummy_field: str=None


    def nexus_resource_constructor(this) -> dict:
        
        nexus_dict = {}

        nexus_dict['@id'] = this.acquisition_object_uri

        nexus_dict["@context"] = [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            {
                "fhir": "http://hl7.org/fhir/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "dcterms": "http://purl.org/dc/terms/",
                "prov": "http://www.w3.org/ns/prov#",
                "sio": "http://semanticscience.org/resource/",
                "nidm": "http://purl.org/nidash/nidm#",
                "DICOM": "http://uri.interlex.org/dicom/uris/terms/",
                "bids": "https://wolfborg.github.io/nidm/bids.html#",
                "gnmd": "https://genemede.github.io/genemede_glossary.html#"
            }
        ]

        nexus_dict["@type"] = [
            "prov:Entity",
            "nidm:AcquisitionObject"
        ]

        nexus_dict['prov:wasGeneratedBy'] = {
            "@id": this.acquisition_uri
        }

        nexus_dict['nidm:AcquisitionObjectQuality'] = this.acquisition_object_quality

        dicom_context = {}

        if this.dicom_header:
            # SQLite stores dictionaries as string.  Need to convert it to a dict
            this.dicom_header = ast.literal_eval(this.dicom_header)

        if this.dicom_header:
            for dicom_tag in this.dicom_header:
                if dicom_tag['desc'] != '?':
                    dicom_context[dicom_tag['desc']] = f'DICOM:{dicom_tag["tag1"].replace(",","_").lstrip("(").rstrip(")")}'
                    nexus_dict[dicom_tag['desc']] = dicom_tag['value']
                else:
                    dicom_context[dicom_tag['tag1']] = f'DICOM:{dicom_tag["tag1"].replace(",","_").lstrip("(").rstrip(")")}'
                    nexus_dict[dicom_tag['tag1']] = dicom_tag['value']

        if this.non_dicom_header:
            # SQLite stores dictionaries as string.  Need to convert it to a dict
            this.non_dicom_header = ast.literal_eval(this.non_dicom_header)

        if this.non_dicom_header:
            for non_dicom_tag in this.non_dicom_header:
                nexus_dict[non_dicom_tag] = this.non_dicom_header[non_dicom_tag]
            
        nexus_dict['@context'].append(dicom_context)

        nexus_dict["rdfs:label"] = f"Session - {this.session_id}; Acquisition Scan ID - {this.acquisition_id}"

        return nexus_dict