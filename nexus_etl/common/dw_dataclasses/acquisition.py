from dataclasses import dataclass
from dw_dataclasses.base import Base
import datetime
import ast

@dataclass
class acquisition(Base):
    """acquisition"""
    src_system: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_subject_id: str=None
    session_id: str=None
    session_type: str=None
    acquisition_id: str=None
    acquisition_type: str=None
    acquisition_modality: str=None
    acquisition_insert_date: datetime.datetime=None
    acquisition_last_modified: datetime.datetime=None
    acquisition_object_quality: str=None
    device_manufacturer: str=None
    device_name: str=None
    session_date: datetime.datetime=None
    research_study_uri: str=None
    research_study_title: str=None
    research_subject_uri: str=None
    session_uri: str=None
    device_uri: str=None
    acquisition_uri: str=None
    accession_id: str=None
    acquisition_start_date: datetime.datetime=None
    acquisition_start_time: str=None
    series_description: str=None
    xnat_custom_fields: str=None


    def nexus_resource_constructor(this) -> dict:
        
        # convert dictionaries that are stored as strings in SQLite as actual Python Dictionaries
        if this.xnat_custom_fields:
            this.xnat_custom_fields = ast.literal_eval(this.xnat_custom_fields)

        # continue with the creation of nexus dictionary
        nexus_dict = {}
        
        nexus_dict["@id"] = this.acquisition_uri
        
        nexus_dict["@context"] = [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            {
                "fhir": "http://hl7.org/fhir/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "dcterms": "http://purl.org/dc/terms/",
                "prov": "http://www.w3.org/ns/prov#",
                "sio": "http://semanticscience.org/resource/",
                "nidm": "http://purl.org/nidash/nidm#"
            }
        ]
        
        nexus_dict["@type"] = [
            "prov:Activity",
            "nidm:Acquisition"
        ]
        
        nexus_dict["dcterms:identifier"] = this.acquisition_id
        
        
        if this.series_description:
            nexus_dict["dcterms:description"] = this.series_description
        
        
        if this.acquisition_start_date:
            if this.acquisition_start_time:
                start_datetime = f"{str(this.acquisition_start_date.date())} {str(this.acquisition_start_time)}"
            else:
                start_datetime = str(this.acquisition_start_date.date())
            nexus_dict['prov:startedAtTime'] = start_datetime
            
            
        nexus_dict["dcterms:isPartOf"] = {
            "@id": this.session_uri
        }
        
        if this.acquisition_modality:
            nexus_dict['nidm:hadAcquisitionModality'] = this.acquisition_modality

        nexus_dict["prov:wasAssociatedWith"] = []
        nexus_dict["prov:qualifiedAssociation"] = []
        
        # capture the assocation with the research subject
        
        nexus_dict["prov:wasAssociatedWith"].append({
            "@id": this.research_subject_uri
        })
        
        nexus_dict["prov:qualifiedAssociation"].append({
            "@type": "prov:Association",
            "prov:agent": {
                "@id": this.research_subject_uri
            },
            "prov:hadRole": {
                "@id": "sio:SIO_000399"
            },
            "rdfs:comment": "This was the subject of the acquisition activity."
        })
        
        if this.device_uri:
            # capture the association with the device
            nexus_dict["prov:wasAssociatedWith"].append({
                "@id": this.device_uri
            })

            nexus_dict["prov:qualifiedAssociation"].append({
                "@type": "prov:Association",
                "prov:agent": {
                    "@id": this.device_uri
                },
                "prov:hadRole": {
                    "@id": "sio:SIO_000956"
                },
                "rdfs:comment": "This was the device user for the acquisition activity."
            })
        
        nexus_dict["rdfs:label"] = f"Session - {this.session_id}; Acquisition Scan ID - {this.acquisition_id}"

        return nexus_dict