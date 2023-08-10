from dataclasses import dataclass
from dw_dataclasses.base import Base


@dataclass
class questionnaire_response(Base):
    src_system: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_study_uri: str=None
    research_study_title: str=None
    questionnaire_uri: str=None
    questionnaire_label: str=None
    response_subject_uri: str=None
    response_subject_id: str=None
    response_text: str=None
    question_type: str=None
    questionnaire_item_uri: str=None
    question_label: str=None
    response_code: str=None
    response_code_display: str=None
    response_code_uri: str=None
    questionnaire_response_uri: str=None
    questionnaire_response_item_uri: str=None
    question_group_id: str=None
    question_group_uri: str=None
    response_group_uri: str=None
    question_id: str=None
    required_flag: str=None
    response_list_group_uri: str=None
    response_index_in_list: str=None
    questionnaire_uuid: str=None
    response_subject_type: str=None
    subject_type: str=None
    xnat_data_type: str=None


    def nexus_resource_constructor(this) -> dict:
        
        nexus_dict = {}
        
        nexus_dict["@id"] = this.questionnaire_response_item_uri

        nexus_dict["@type"] = [
            "fhir:QuestionnaireResponseItemComponent",
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

        if this.response_group_uri and this.response_list_group_uri:
            # this row is part of a list and also in a gorup
            nexus_dict["@reverse"] = {
                "fhir:QuestionnaireResponse.item.item": {
                    "@id": this.response_list_group_uri,
                    "@reverse": {
                        "fhir:QuestionnaireResponse.item.item": {
                            "@id": this.response_group_uri
                        }
                    }
                }
            }
        elif this.response_group_uri:
            # only part of a group, not also a list
            nexus_dict['@reverse'] = {
                "fhir:QuestionnaireResponse.item.item": {
                    "@id": this.response_group_uri
                }
            }
        
        elif this.response_list_group_uri:
            # only part of a list, but not a group i.e. container, data grid, etc.
            
            nexus_dict['@reverse'] = {
                "fhir:QuestionnaireResponse.item.item": {
                    "@id": this.response_list_group_uri,
                    "@reverse": {
                        "fhir:QuestionnaireResponse.item": {
                            "@id": this.questionnaire_response_uri
                        }
                    }
                }
#                 "fhir:QuestionnaireResponse.item.item": {
#                     "@id": this.response_list_group_uri
#                 }
            }
            
        else:
            # no group or list therefore attach it directly to the questionnaire response
            nexus_dict['@reverse'] = {
                "fhir:QuestionnaireResponse.item": {
                    "@id": this.questionnaire_response_uri
                }
            }
        
        
        if this.question_group_id:
            question_id_text = f'{this.question_group_id}.{this.question_id}'
        else:
            question_id_text = this.question_id
            
        nexus_dict['fhir:QuestionnaireResponse.item.linkId'] = {
            "fhir:value": question_id_text
        }

        if this.questionnaire_item_uri:
            nexus_dict['fhir:QuestionnaireResponse.item.definition'] = {
                "@id": this.questionnaire_item_uri
            }

        if this.question_label:
            nexus_dict['fhir:QuestionnaireResponse.item.text'] = {
                "fhir:value": this.question_label
            }

        if this.response_text:    
            if this.question_type:
                if this.question_type == 'textfield':
                    nexus_dict['fhir:QuestionnaireResponse.item.answer'] = {
                        "fhir:QuestionnaireResponse.item.answer.valueString": {
                            "fhir:value": this.response_text
                        }
                    }
                    
                elif this.question_type in ['selectboxes','xnatSelect']:
                    nexus_dict['fhir:QuestionnaireResponse.item.answer'] = {
                        "fhir:QuestionnaireResponse.item.answer.valueCoding": {
                            "@type": "fhir:Coding",
                            "fhir:code": {
                                "@type": "fhir:code",
                                "fhir:value": this.response_code
                            },
                            "fhir:display": {
                                "@type": "fhir:string",
                                "fhir:value": this.response_code_display
                            }
                        }
                    }
                elif this.question_type in ['datetime']:
                    nexus_dict['fhir:QuestionnaireResponse.item.answer'] = {
                        "fhir:QuestionnaireResponse.item.answer.valueDateTime": {
                            "@type": "fhir:dateTime",
                            "fhir:value": this.response_text
                        }
                    }
                
                elif this.question_type in ['url']:
                    nexus_dict['fhir:QuestionnaireResponse.item.answer'] = {
                        "fhir:QuestionnaireResponse.item.answer.valueUrl": {
                            "@type": "fhir:url",
                            "fhir:value": this.response_text
                        }
                    }
                else:
                    nexus_dict['fhir:QuestionnaireResponse.item.answer'] = {
                        "fhir:QuestionnaireResponse.item.answer.valueString": {
                            "fhir:value": this.response_text
                        }
                    }
            else:
                nexus_dict['fhir:QuestionnaireResponse.item.answer'] = {
                        "fhir:QuestionnaireResponse.item.answer.valueString": {
                            "fhir:value": this.response_text
                        }
                    }

        nexus_dict["prov:wasGeneratedBy"] = {
            "@id": this.research_study_uri
        }
        
        return nexus_dict
