from dataclasses import dataclass,field
from dw_dataclasses.base import Base
import datetime


@dataclass
class questionnaire_list(Base):
    src_system: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_study_title: str=None
    research_study_uri: str=None
    questionnaire_uri: str=None
    questionnaire_name: str=None
    questionnaire_title: str=None
    subject_type: str=None
    questionnaire_uuid: str=None
    xnat_data_type: str=None

    def nexus_resource_constructor(this) -> dict:
        
        nexus_dict = {}
        
        nexus_dict["@id"] = this.questionnaire_uri

        nexus_dict["@type"] = [
            "fhir:Questionnaire",
            "prov:Entity"
        ]

        nexus_dict["@context"] = [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            {
                "fhir": "http://hl7.org/fhir/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "prov": "https://www.w3.org/TR/prov-o/"
            }
        ]

        nexus_dict["rdfs:label"] = this.questionnaire_title
        
        nexus_dict["fhir:Questionnaire.name"] = {
            "@type": "fhir:string",
            "fhir:value": this.questionnaire_name
        }
        
        nexus_dict["fhir:Questionnaire.identifier"] = {
            "@type": "fhir:Identifier",
            "fhir:Identifier.value": {
                "@type": "fhir:string",
                "fhir:value": this.questionnaire_title
            }
        }
        
        nexus_dict["fhir:Questionnaire.title"] = {
            "@type": "fhir:string",
            "fhir:value": this.questionnaire_title
        }
        
        nexus_dict["fhir:Questionnaire.subjectType"] = {
            "@type": "fhir:code",
            "fhir:value": this.subject_type
        }
        
        nexus_dict["prov:wasGeneratedBy"] = {
            "@id": this.research_study_uri
        }
        
        return nexus_dict