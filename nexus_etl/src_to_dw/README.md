These scripts are used to load the data to SQLite.

The order the scripts need to run is:

    1. load_research_study.py
    2. load_research_subject.py
    3. load_device.py
    4. load_session.py
    5. load_acquisition.py
    6. load_acquisition_object.py
    7. load_questionnaire_list.py
    8. load_questionnaire_item_list.py
    9. load_questionnaire_item_options.py
    10. load_questionnaire_response_list.py
    11. load_questionnaire_response_projects.py
    12. load_questionnaire_response_subjects.py
    13. load_questionnaire_response_sessions.py

The command for running these scripts is:
```bash
python [script_name] [xnat project id]
```
