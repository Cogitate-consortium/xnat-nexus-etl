# makefile to perform all actions necessary to ETL data from xnat to nexus

# Oneshell to run multiple lines in a recipe in the same shell,
# so we don't have to chain commands together with semicolon
# .ONESHELL does not work for macos GNU make 3.8.1 (brew install make to upgrade)
.ONESHELL:

# need to specify bash in order for conda activate to work.
SHELL = /bin/bash

# provide the path to the correct python environment
PYTHON = /Users/praveen.sripad/cogcode/xnat-nexus-etl/venv/bin/python

PROJECTS = nexus1 nexus2 nexus3

# provide the base directory where the nexus_etl scripts are located
BASEDIR = /Users/praveen.sripad/cogcode/xnat-nexus-etl/nexus_etl

# provide paths to the docker compose file and local directory
# the directory is required to correctly find the nginx.conf file
NEXUS_DOCKER_CONFIG = /Users/praveen.sripad/mpi/nexus/nexus_install/docker-compose.yaml
NEXUS_DOCKER_DIR = /Users/praveen.sripad/mpi/nexus/nexus_install/

SRCTODW = $(BASEDIR)/src_to_dw
DWTONEXUS = $(BASEDIR)/dw_to_nexus

quicktest:
	@echo $(PYTHON)
	
	@echo "run one script from src_to_dw"
	for project in $(PROJECTS) ; do \
		$(PYTHON) $(SRCTODW)/load_research_study.py $$project ; \
	done
		
	@echo "run one script from dw_to_nexus"
	$(PYTHON) $(DWTONEXUS)/Acquisition.py

database:
	if [ ! -f $(BASEDIR)/database/mpg_eln.db ]; then
		echo "Creating database"
		$(PYTHON) $(BASEDIR)/database/initialize_database.py
	fi

docker:
	# start nexus locally via docker
	@echo "Starting NEXUS locally via docker"
	cd $(NEXUS_DOCKER_DIR); docker compose --project-name nexus --project-directory $(NEXUS_DOCKER_DIR) --file $(NEXUS_DOCKER_CONFIG) up --detach

src_to_dw:
	@echo $(PWD)
	@echo "Running ETL to load data from XNAT to local db"
	for project in $(PROJECTS) ; do \
		echo $$project ; \
		$(PYTHON) $(SRCTODW)/load_research_study.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_research_subject.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_device.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_session.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_acquisition.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_acquisition_object.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_questionnaire_list.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_questionnaire_item_list.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_questionnaire_item_options.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_questionnaire_response_list.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_questionnaire_response_projects.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_questionnaire_response_subjects.py $$project ; \
		$(PYTHON) $(SRCTODW)/load_questionnaire_response_sessions.py $$project ; \
	done

dw_to_nexus:
	@echo $(PWD)
	@echo "Running ETL to load data from local db to NEXUS"
	for project in $(PROJECTS) ; do \
		echo $$project ; \
		$(PYTHON) $(DWTONEXUS)/Acquisition.py ; \
		$(PYTHON) $(DWTONEXUS)/AcquisitionObject.py ; \
		$(PYTHON) $(DWTONEXUS)/Device.py ; \
		$(PYTHON) $(DWTONEXUS)/Questionnaire.py ; \
		$(PYTHON) $(DWTONEXUS)/QuestionnaireItemList.py ; \
		$(PYTHON) $(DWTONEXUS)/QuestionnaireItemOptions.py ; \
		$(PYTHON) $(DWTONEXUS)/QuestionnaireList.py ; \
		$(PYTHON) $(DWTONEXUS)/QuestionnaireResponse.py ; \
		$(PYTHON) $(DWTONEXUS)/QuestionnaireResponseItems.py ; \
		$(PYTHON) $(DWTONEXUS)/ResearchStudy.py ; \
		$(PYTHON) $(DWTONEXUS)/ResearchSubject.py ; \
		$(PYTHON) $(DWTONEXUS)/Session.py ; \
	done

# under dw_to_nexus
# $(PYTHON) $(DWTONEXUS)/DeprecateResources.py ; \

all: database docker src_to_dw dw_to_nexus