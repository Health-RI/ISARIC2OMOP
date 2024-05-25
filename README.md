# ISARIC2OMOP

Scripts to create and populate OMOP DB from ISARIC-formatted data.

### Database set up

Source of .sql scripts for OMOP DB set up: [OMOP CDM v5.4](https://github.com/OHDSI/CommonDataModel/tree/v5.4.0/inst/ddl/5.4/sql_server).
It is also possible to set up a DB with R scripts as per [documentation](https://github.com/OHDSI/CommonDataModel/tree/v5.4.0).

Alternatively use current repository and perform following steps:
1. Clone the repo
2. Copy [.env.example](.env.example) to .env file and set variable accordingly
3. Download vocabularies from [Athena](https://athena.ohdsi.org/vocabulary/list)
4. Place the vocabularies archive under [./database](./database/) directory as `vocabulary_download_v5.tar.gz`
5. Run
```commandline
 docker compose up -d --build
```
In the database there should be two schemas: `cdm` for OMOP data and `vocabularies` prepopulated with concepts data
from the downloaded vocabularies

### ISARIC-formatted files to OMOP

Conversion of ISARIC data was implemented based on the following [mapping](https://lygatureprojectplaza.sharepoint.com/:x:/r/sites/by_covid/Project%20Documents/Beacon/COVID-NL_OMOP.xlsx?d=wb7c9ea1c911946b79c957bd9fc72271b&csf=1&web=1&e=uraYrh), 
[ISARIC Data Dictonary Codebook](http://capacity-covid.eu/wp-content/uploads/CAPACITY-REDCap-2.pdf)
[OMOP CDM data model specification](https://omop-erd.surge.sh/omop_cdm/index.html) and 
[ISARIC mapping guid](https://github.com/globaldothealth/isaric/blob/main/docs/guide.rst).

**NB!** Mapping was made based on scrambled data and must be checked when switching to real data. 

