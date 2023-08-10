import hashlib

def generate_sparql_compliant_variable_name(variable_name):
    
    # the grammar for sparql: https://www.w3.org/TR/sparql11-query/#rVARNAME
    
    variable_name = variable_name.replace('-','_')
    variable_name = variable_name.replace('.','___')
    variable_name = variable_name.replace(' ','___')
    
    variable_name = f'?{variable_name}'
    
    return variable_name

def generate_question_hash(question_name):
    hash = hashlib.sha1(question_name.encode("UTF-8")).hexdigest()
    return hash

def query_constructor(response_subject_uri_field_name, question_name, sparql_compliant_variable_name):

    question_hash = generate_question_hash(question_name)
    
    query_string = f"""
        OPTIONAL {{
            ?{question_hash}_item a fhir:QuestionnaireResponseItemComponent .
            ?{question_hash}_item fhir:QuestionnaireResponse.item.text/fhir:value ?{question_hash}_subject_lvl_question_label .
            FILTER (?{question_hash}_subject_lvl_question_label = '{question_name}') .            
            ?{question_hash}_item fhir:QuestionnaireResponse.item.answer/(<>|!<>)/fhir:value {sparql_compliant_variable_name} .

            ?{question_hash}_response fhir:QuestionnaireResponse.item [ fhir:QuestionnaireResponse.item.item* ?{question_hash}_item ] .
            ?{question_hash}_response fhir:QuestionnaireResponse.subject/fhir:link {response_subject_uri_field_name} .
            
            ?{question_hash}_item nxv:deprecated false .
            FILTER ( NOT EXISTS {{ ?{question_hash}_item nxv:deprecated true }} ) .
            
        }}
    """

    return query_string
    