import setup
import nexussdk as nexus


config = setup.config
nexus = setup.nexus

# create the organization
nexus.organizations.create('MPI')

# create the project
nexus.projects.create('MPI', 'ELN')