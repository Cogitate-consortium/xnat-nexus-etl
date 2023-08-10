from dataclasses import dataclass
from dw_dataclasses.base import Base


@dataclass
class questionnaire_response_list(Base):
    src_system: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_study_uri: str=None
    research_study_title: str=None
    questionnaire_uri: str=None
    questionnaire_label: str=None
    response_subject_uri: str=None
    response_subject_id: str=None
    questionnaire_response_uri: str=None
    questionnaire_uuid: str=None
    response_subject_type: str=None
    subject_type: str=None
    xnat_data_type: str=None

    def nexus_resource_constructor(this) -> dict:
        
        nexus_dict = {}
        
        nexus_dict["@id"] = this.questionnaire_response_uri

        nexus_dict["@type"] = [
            "fhir:QuestionnaireResponse",
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

        nexus_dict["rdfs:label"] = f"Response for {this.questionnaire_label}"
        
        nexus_dict["fhir:QuestionnaireResponse.Questionnaire"] = {
            "@id": this.questionnaire_uri
        }
        
        nexus_dict["fhir:QuestionnaireResponse.status"] = {
            "@type": "fhir:code",
            "fhir:value": "completed"
        }
        
        nexus_dict["fhir:QuestionnaireResponse.subject"] = {
            "@type": "fhir:Reference",
            "fhir:Reference.reference": {
                "@type": "fhir:string",
                "fhir:value": this.response_subject_uri
            },
            "fhir:link": {
                "@id": this.response_subject_uri
            }
        }

        nexus_dict["prov:wasGeneratedBy"] = {
            "@id": this.research_study_uri
        }
        
        return nexus_dict