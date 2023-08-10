# About
This folder contains the data classes used for this project. The data classes represent the resource types that have been used to capture the data from XNAT.

| XNAT Resource Type 	                                    | Data Class 	                |
|--------------------	                                    |-------------------------------|
|DICOM Headers and other header resources                   |Acquisition Object            	|
|Images                    	                                |Acquisition            	    |
|Device                    	                                |Device            	            |
|List of custom form questions and groups                   |Questionnaire Item List        |
|List of custom form                    	                |Questionnaire List            	|
|List of unique responses                                   |Questionnaire Response List    |
|Response values                                            |Questionnaire Response         |
|Full questionnaire with questions and response options.    |Questionnaire                  |
|XNAT Project                                               |Research Study                 |
|XNAT Subject/study participant                             |Research Subject               |
|XNAT imaging session                                       |Session                        |


Within each data class file, there is a nexus_resource_constructor function that converts the data class attributes to a JSON-LD resource that can be inserted into Blue Brain Nexus.

The vocabulary used in the JSON-LD files is:

```json
{
    "@context" : [
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
}
```
