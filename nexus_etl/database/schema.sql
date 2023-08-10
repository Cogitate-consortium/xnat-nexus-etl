CREATE TABLE xnat_config
(
    xnat_project_id text NULL,
    nexus_xnat_last_successful_load_ts datetime NULL
);


CREATE TABLE research_study
(
    src_system text NULL,
    research_study_uri text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_study_title text NULL,
    research_study_site text NULL,
    research_study_category text NULL,
    research_study_description text NULL,
    research_study_start_date DATETIME NULL,
    research_study_end_date DATETIME NULL,
    research_study_principal_investigator text NULL,
    research_study_status text NULL,
    "_createdat" DATETIME NULL,
    "_updatedat" DATETIME NULL,
    "_rev" INTEGER NULL,
    xnat_custom_fields TEXT NULL
);


CREATE TABLE acquisition
(
    src_system text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_subject_id text NULL,
    session_id text NULL,
    session_type text NULL,
    acquisition_uri text NULL,
    acquisition_id text NULL,
    acquisition_type text NULL,
    acquisition_insert_date datetime NULL,
    acquisition_last_modified datetime NULL,
    acquisition_object_quality text NULL,
    device_manufacturer text NULL,
    device_name text NULL,
    session_date datetime NULL,
    research_study_uri text NULL,
    research_study_title text NULL,
    research_subject_uri text NULL,
    session_uri text NULL,
    device_uri text NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL,
    accession_id text NULL,
    acquisition_start_date text NULL,
    acquisition_start_time text NULL,
    series_description text NULL,
    xnat_custom_fields text NULL,
    acquisition_modality text NULL
);



CREATE TABLE acquisition_object
(
    src_system text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_subject_id text NULL,
    session_id text NULL,
    session_type text NULL,
    acquisition_uri text NULL,
    acquisition_id text NULL,
    acquisition_type text NULL,
    acquisition_insert_date datetime NULL,
    acquisition_last_modified datetime NULL,
    acquisition_object_quality text NULL,
    device_manufacturer text NULL,
    device_name text NULL,
    session_date datetime NULL,
    research_study_uri text NULL,
    research_study_title text NULL,
    research_subject_uri text NULL,
    session_uri text NULL,
    device_uri text NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL,
    accession_id text NULL,
    acquisition_start_date text NULL,
    acquisition_start_time text NULL,
    series_description text NULL,
    xnat_custom_fields text NULL,
    dicom_header text NULL,
    acquisition_object_uri text NULL,
    non_dicom_header text NULL,
    dummy_field text NULL
);





CREATE TABLE device
(
    src_system text NULL,
    device_uri text NULL,
    device_manufacturer text NULL,
    device_name text NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL
);


CREATE TABLE nexus_etl_log
(
    resource_type text NULL,
    last_success_load_ts datetime NULL
);



CREATE TABLE questionnaire
(
    src_system text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_study_title text NULL,
    research_study_uri text NULL,
    questionnaire_name text NULL,
    questionnaire_title text NULL,
    questionnaire_uri text NULL,
    question_id text NULL,
    question_label text NULL,
    question_type text NULL,
    required_flag text NULL,
    questionnaire_item_uri text NULL,
    answer_option_code text NULL,
    answer_option_display text NULL,
    questionnaire_item_answer_option_uri text NULL,
    "_createdat" datetime NULL,
    "_rev" integer NULL,
    "_updatedat" datetime NULL,
    question_description text NULL,
    group_id text NULL,
    group_uri text NULL,
    questionnaire_uuid text NULL,
    subject_type text NULL,
    xnat_data_type text NULL
);



CREATE TABLE questionnaire_item_list
(
    src_system text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_study_title text NULL,
    research_study_uri text NULL,
    question_id text NULL,
    question_label text NULL,
    question_type text NULL,
    questionnaire_item_uri text NULL,
    questionnaire_name text NULL,
    questionnaire_title text NULL,
    questionnaire_uri text NULL,
    required_flag numeric NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL,
    question_description text NULL,
    group_id text NULL,
    group_uri text NULL,
    enable_when_question text NULL,
    enable_when_operator text NULL,
    enable_when_answer text NULL,
    "validate" text NULL,
    repeats_flag numeric NULL,
    questionnaire_uuid text NULL,
    subject_type text NULL,
    xnat_data_type text NULL
);


CREATE TABLE questionnaire_list
(
    src_system text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_study_title text NULL,
    research_study_uri text NULL,
    questionnaire_uri text NULL,
    questionnaire_name text NULL,
    questionnaire_title text NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL,
    subject_type text NULL,
    questionnaire_uuid text NULL,
    xnat_data_type text NULL
);



CREATE TABLE questionnaire_response
(
    src_system text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_study_uri text NULL,
    research_study_title text NULL,
    questionnaire_uri text NULL,
    questionnaire_label text NULL,
    response_subject_uri text NULL,
    response_subject_id text NULL,
    response_text text NULL,
    question_type text NULL,
    questionnaire_item_uri text NULL,
    question_label text NULL,
    response_code text NULL,
    response_code_display text NULL,
    response_code_uri text NULL,
    questionnaire_response_uri text NULL,
    questionnaire_response_item_uri text NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL,
    question_group_id text NULL,
    question_group_uri text NULL,
    response_group_uri text NULL,
    question_id text NULL,
    required_flag numeric NULL,
    response_list_group_uri text NULL,
    response_index_in_list text NULL,
    questionnaire_uuid text NULL,
    response_subject_type text NULL,
    subject_type text NULL,
    xnat_data_type text NULL
);



CREATE TABLE questionnaire_response_list
(
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_study_uri text NULL,
    research_study_title text NULL,
    questionnaire_uri text NULL,
    questionnaire_label text NULL,
    response_subject_uri text NULL,
    response_subject_id text NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL,
    questionnaire_response_uri text NULL,
    src_system text NULL,
    questionnaire_uuid text NULL,
    response_subject_type text NULL,
    subject_type text NULL,
    xnat_data_type text NULL
);



CREATE TABLE research_subject
(
    src_system text NULL,
    research_subject_uri text NULL,
    research_subject_id text NULL,
    research_study_uri text NULL,
    research_study_title text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL,
    xnat_custom_fields text NULL
);



CREATE TABLE "session"
(
    src_system text NULL,
    research_study_id text NULL,
    research_study_id_type text NULL,
    research_study_title text NULL,
    research_study_uri text NULL,
    research_subject_id text NULL,
    research_subject_uri text NULL,
    session_uri text NULL,
    session_id text NULL,
    session_type text NULL,
    session_date datetime NULL,
    "_createdat" datetime NULL,
    "_updatedat" datetime NULL,
    "_rev" integer NULL,
    accession_id text NULL,
    xnat_custom_fields text NULL
);