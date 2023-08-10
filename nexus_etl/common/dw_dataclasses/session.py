from dataclasses import dataclass
from dw_dataclasses.base import Base
import datetime
import ast

@dataclass
class session(Base):
    """data_warehouse.session"""
    src_system: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_study_title: str=None
    research_study_uri: str=None
    research_subject_id: str=None
    research_subject_uri: str=None
    session_id: str=None
    accession_id: str=None
    session_type: str=None
    session_date: datetime.datetime=None
    session_uri: str=None
    xnat_custom_fields: object=None

        
    def nexus_resource_constructor(this) -> dict:
        
        # convert dictionaries that are stored as strings in SQLite as actual Python Dictionaries
        if this.xnat_custom_fields:
            this.xnat_custom_fields = ast.literal_eval(this.xnat_custom_fields)

        # continue with the creation of nexus dictionary

        nexus_dict = {}
        
        nexus_dict["@id"] = this.session_uri

        nexus_dict["@type"] = [
            "prov:Activity",
            "nidm:Session"
        ]

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
        
        nexus_dict["rdfs:label"] = this.session_id
        
        nexus_dict["dcterms:identifier"] = this.session_id
        
        nexus_dict["dcterms:isPartOf"] = {
            "@id": this.research_study_uri
        }
        
        nexus_dict["prov:qualifiedAssociation"] = {
            "@type": "prov:Association",
            "prov:agent": {
                "@id": this.research_subject_uri
            },
            "prov:hadRole": {
                "@id": "sio:Subject"
            }
        }
        
        if this.session_date:
            nexus_dict["prov:startedAtTime"] = str(this.session_date)
        
        nexus_dict["dcterms:Identifier"] = this.session_id
        
        nexus_dict["prov:wasAssociatedWith"] = {
            "@id": this.research_subject_uri
        }
        
        return nexus_dict
        
        
