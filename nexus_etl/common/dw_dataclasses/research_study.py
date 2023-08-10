from dataclasses import dataclass
from dw_dataclasses.base import Base
import datetime
import ast

@dataclass
class research_study(Base):
    src_system: str=None
    research_study_uri: str=None
    research_study_id: str=None
    research_study_id_type: str=None
    research_study_title: str=None
    research_study_site: str=None
    research_study_category: str=None
    research_study_description: str=None
    research_study_start_date: datetime.datetime=None
    research_study_end_date: datetime.datetime=None
    research_study_principal_investigator: str=None
    research_study_status: str=None
    xnat_custom_fields: object=None
    
    def nexus_resource_constructor(this) -> dict:
        
        # convert dictionaries that are stored as strings in SQLite as actual Python Dictionaries
        if this.xnat_custom_fields:
            this.xnat_custom_fields = ast.literal_eval(this.xnat_custom_fields)

        # continue with the creation of nexus dictionary

        nexus_dict = {} 
        
        nexus_dict["@id"] = this.research_study_uri

        nexus_dict["@type"] = [
            "fhir:ResearchStudy",
            "prov:Activity"
        ]

        nexus_dict["@context"] = [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            {
                "fhir": "http://hl7.org/fhir/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "prov": "https://www.w3.org/TR/prov-o/"
            }
        ]

        nexus_dict["rdfs:label"] = this.research_study_id

        nexus_dict["fhir:ResearchStudy.identifier"] = {
            "@type": "fhir:Identifier",
            "fhir:Identifier.value":{
                "fhir:value":this.research_study_id
            },
            "fhir:Identifier.type": {
                "@type":"fhir:CodeableConcept",
                "fhir:CodeableConcept.text": {
                    "fhir:value": this.research_study_id_type
                }
            },
            "fhir:Identifier.system":{
                "@type": "fhir:uri",
                "fhir:value": "https://xnat.prj.ae.mpg.de"
            }
        }
        if this.research_study_title:
            nexus_dict["fhir:ResearchStudy.title"] = {
                "@type": "fhir:string",
                "fhir:value": this.research_study_title
            }

        if this.research_study_site:
            nexus_dict["fhir:ResearchStudy.site"] = {
                "@type":"fhir:Reference",
                "fhir:Reference.display":{
                    "fhir:value": this.research_study_site
                }
            }

        if this.research_study_category:
            nexus_dict["fhir:ResearchStudy.category"] = {
                "@type":"fhir:CodeableConcept",
                "fhir:CodeableConcept.text": {
                    "fhir:value": this.research_study_category
                }
            }

        if this.research_study_description:
            nexus_dict['fhir:ResearchStudy.description'] = {
                "@type":"fhir:markdown",
                "fhir:value": this.research_study_description
            }

        start_date_dict = {}
        if this.research_study_start_date:
            start_date_dict = {
                "@type": "fhir:dateTime",
                "fhir:value": this.research_study_start_date
            }

        end_date_dict = {}
        if this.research_study_end_date:
            start_date_dict = {
                "@type": "fhir:dateTime",
                "fhir:value": this.research_study_end_date
            }

        if start_date_dict or end_date_dict:
            nexus_dict['fhir:ResearchStudy.period'] = {
                "@type": "fhir:Period"
            }

            if start_date_dict:
                nexus_dict["fhir:Period.start"] = start_date_dict

            if end_date_dict:
                nexus_dict["fhir:Period.end"] = end_date_dict

        if this.research_study_principal_investigator:
            nexus_dict['fhir:ResearchStudy.principalInvestigator'] = {
                "@type":"fhir:Reference",
                "fhir:Reference.display":{
                    "fhir:value": this.research_study_principal_investigator
                }
            }

        if this.research_study_status:
            nexus_dict['fhir:ResearchStudy.status'] = {
                "@type": "fhir:code",
                "fhir:value":this.research_study_status
            }

        return nexus_dict