from dataclasses import dataclass,field
from dw_dataclasses.base import Base
import datetime


@dataclass
class questionnaire(Base):
    src_system: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_study_title: str=None
    research_study_uri: str=None
    questionnaire_name: str=None
    questionnaire_title: str=None
    questionnaire_uri: str=None
    question_id: str=None
    question_label: str=None
    question_type: str=None
    required_flag: str=None
    questionnaire_item_uri: str=None
    answer_option_code: str=None
    answer_option_display: str=None
    questionnaire_item_answer_option_uri: str=None
    question_description: str=None
    group_id: str=None
    group_uri: str=None
    questionnaire_uuid: str=None
    subject_type: str=None
    xnat_data_type: str=None

    def nexus_resource_constructor(this) -> dict:
        
        nexus_dict = {}
        
        nexus_dict["@id"] = this.questionnaire_item_answer_option_uri

        nexus_dict["@type"] = "fhir:QuestionnaireItemComponent.QuestionnaireItemAnswerOptionComponent"

        nexus_dict["@context"] = [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            {
                "fhir": "http://hl7.org/fhir/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            }
        ]

        nexus_dict["rdfs:label"] = this.answer_option_code
        
        # itemPrefix = "fhir:Questionnaire.item"
        # if this.group_id:
        #     itemCount = len(this.group_id.split("."))
        #     itemPrefix += ".item" * itemCount
            
        nexus_dict['@reverse'] = {
            "fhir:Questionnaire.item.answerOption": {
                "@id": this.questionnaire_item_uri
            }
        }
        
        nexus_dict["fhir:Questionnaire.item.answerOption.valueCoding"] = {
            "@type": "fhir:Coding",
            "fhir:Coding.code": {
                "@type": "fhir:code",
                "fhir:value": this.answer_option_code
            },
            "fhir:Coding.display": {
                "fhir:value": this.answer_option_display
            }
        }

        return nexus_dict