from dataclasses import dataclass
from dw_dataclasses.base import Base
import ast

@dataclass
class research_subject(Base):
    """data_warehouse.research_subject"""
    src_system: str=None
    research_subject_uri: str=None
    research_subject_id: str=None
    research_study_uri: str=None
    research_study_title: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    xnat_custom_fields: object=None

        
    def nexus_resource_constructor(this) -> dict:
        
        # convert dictionaries that are stored as strings in SQLite as actual Python Dictionaries
        if this.xnat_custom_fields:
            this.xnat_custom_fields = ast.literal_eval(this.xnat_custom_fields)

        # continue with the creation of nexus dictionary

        nexus_dict = {}
        
        nexus_dict["@id"] = this.research_subject_uri

        nexus_dict["@type"] = "fhir:ResearchSubject"

        nexus_dict["@context"] = [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            {
                "fhir": "http://hl7.org/fhir/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            }
        ]

        nexus_dict["rdfs:label"] = this.research_subject_id      
        
        nexus_dict["fhir:ResearchSubject.identifier"] = {
            "@type": "fhir:Identifier",
            "fhir:Identifier.value":{
                "fhir:value":this.research_subject_id
            },
            "fhir:Identifier.system":{
                "@type": "fhir:uri",
                "fhir:value": "https://xnat.prj.ae.mpg.de"
            }
        }

        nexus_dict["fhir:ResearchSubject.study"] = {
            "@type": "fhir:Reference",
            "fhir:Reference.reference": {
                "fhir:value": this.research_study_uri
            },
            "fhir:Reference.type": {
                "fhir:value": "http://hl7.org/fhir/ResearchStudy"
            },
            "fhir:link": {
                "@id": this.research_study_uri
            }

        }

        return nexus_dict