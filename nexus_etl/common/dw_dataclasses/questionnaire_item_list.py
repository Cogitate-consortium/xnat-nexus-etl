from dataclasses import dataclass
from dw_dataclasses.base import Base
import ast

@dataclass
class questionnaire_item_list(Base):
    src_system: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_study_title: str=None
    research_study_uri: str=None
    question_id: str=None
    question_label: str=None
    question_type: str=None
    questionnaire_item_uri: str=None
    questionnaire_name: str=None
    questionnaire_title: str=None
    questionnaire_uri: str=None
    required_flag: str=None
    question_description: str=None
    group_id: str=None
    group_uri: str=None
    enable_when_question: str=None
    enable_when_operator: str=None
    enable_when_answer: str=None
    validate: str=None
    repeats_flag: str=None
    questionnaire_uuid: str=None
    subject_type: str=None
    xnat_data_type: str=None


    def nexus_resource_constructor(this) -> dict:
        
        # convert dictionaries that are stored as strings in SQLite as actual Python Dictionaries
        if this.validate:
            this.validate = ast.literal_eval(this.validate)

        # continue with the creation of nexus dictionary

        nexus_dict = {}
        
        nexus_dict["@id"] = this.questionnaire_item_uri

        nexus_dict["@type"] = [
            "fhir:QuestionnaireItemComponent",
            "prov:Entity",
            "sdo:Question"
        ]

        nexus_dict["@context"] = [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            {
                "fhir": "http://hl7.org/fhir/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "sdo": "https://schema.org/",
                "prov": "https://www.w3.org/TR/prov-o/"
            }
        ]

        nexus_dict["rdfs:label"] = this.question_id

        
        #itemPrefix = "fhir:Questionnaire.item"
        if this.group_uri:
            # itemCount = len(this.group_id.split("."))
            # itemPrefix += ".item" * itemCount
            # nexus_dict["@type"][0] += "Item" * itemCount
            nexus_dict["@reverse"] = {
                "fhir:Questionnaire.item.item": {
                    "@id": this.group_uri
                }
            }
        else:
            nexus_dict["@reverse"] = {
                "fhir:Questionnaire.item": {
                    "@id": this.questionnaire_uri
                }
            }
            
        nexus_dict["fhir:Questionnaire.item.linkId"] = {
            "@type": "fhir:string",
            "fhir:value": this.question_id
        }
        nexus_dict["fhir:Questionnaire.item.text"] = {
            "@type": "fhir:string",
            "fhir:value": this.question_label
        }
        nexus_dict["fhir:Questionnaire.item.type"] = {
            "@type": "fhir:code",
            "fhir:value": this.question_type
        }
        nexus_dict["fhir:Questionnaire.item.required"] = {
            "@type": "fhir:boolean",
            "fhir:value": this.required_flag
        }
        

        if this.enable_when_question:
            nexus_dict["fhir:Questionnaire.item.enableWhen"] = {
                "fhir:Questionnaire.item.enableWhen.question": {
                    "@type": "fhir:string",
                    "fhir:value": this.enable_when_question
                },
                "fhir:Questionnaire.item.enableWhen.operator": {
                    "@type": "fhir:code",
                    "fhir:value": this.enable_when_operator
                },
                "fhir:Questionnaire.item.enableWhen.answerString": {
                    "@type": "fhir:string",
                    "fhir:value": this.enable_when_answer
                }
            }
        
        if this.question_description:
            nexus_dict["sdo:description"] = this.question_description
        

        
        return nexus_dict