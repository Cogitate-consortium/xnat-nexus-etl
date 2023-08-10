from dataclasses import dataclass
from dw_dataclasses.base import Base


@dataclass
class device(Base):
    src_system: str=None
    device_uri: str=None
    device_manufacturer: str=None
    device_name: str=None


    def nexus_resource_constructor(this) -> dict:
        
        nexus_dict = {} 
        
        nexus_dict["@id"] = this.device_uri

        nexus_dict["@type"] = [
            "fhir:Device"
        ]

        nexus_dict["@context"] = [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            {
                "fhir": "http://hl7.org/fhir/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "prov": "https://www.w3.org/TR/prov-o/"
            }
        ]

        nexus_dict["rdfs:label"] = f"{this.device_manufacturer} {this.device_name}"

        if this.device_manufacturer:
            nexus_dict["fhir:Device.manufacturer"] = {
                "@type": "fhir:string",
                "fhir:value": this.device_manufacturer
            }

        if this.device_name:
            nexus_dict["fhir:Device.deviceName"] = {
                "fhir:Device.deviceName.name": {
                    "@type": "fhir:string",
                    "fhir:value": this.device_name
                },
                "fhir:Device.deviceName.type": {
                    "@type": "fhir:code",
                    "fhir:value": "user-friendly-name"
                }
            }
            

        return nexus_dict