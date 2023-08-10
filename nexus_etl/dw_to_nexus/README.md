These scripts are used to load the data from SQLite to Blue Brain Nexus.

The order the scripts need to run is:

    1. Device.py
    2. ResearchStudy.py
    3. ResearchSubject.py
    4. Session.py
    5. Acquisition.py
    6. AcquisitionObject.py
    7. Questionnaire.py
    8. QuestionnaireList.py
    9. QuestionnaireItemList.py
    10. QuestionnaireItemOptions.py
    11. QuestionnaireResponse.py
    12. QuestionnaireResponseItems.py

The command for running the scripts is:

```bash
python [script name]
```

For performance reasons, the scripts will load data that has been altered since the last load.  Logs of previous loads are stored in SQLite table 'nexus_etl_log'.  If you need to reload all of the data, you can simply truncate that table (or change whatever dates are needed) and run the scripts.