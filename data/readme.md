# Data
Data was generated using the ETL-CMS repo and then loaded into PostgreSQL using the CommonDataModel repo. High-level instructions are below.

## Obtain Raw Data

To get the data:

1. Clone the ETL-CMS tool from: https://github.com/OHDSI/ETL-CMS/tree/unm-improvements
2. Change to the scripts directory
3. Run the ```get_synpuf_files.py``` script
4. The data will then be downloaded
5. Change to the python_etl directory 
6. Follow the readme in this direcotry to setup your system.
7. Finally, run the ```CMS_SynPUF_ETL_CDM_v5.py``` script to convert 

This will produce valid .csv files ready for import and usage.

## Importing to PostgreSQL

To add the raw data to PostgreSQL

1. Clone the CommonDataModel repo from: https://github.com/OHDSI/CommonDataModel/tree/master/PostgreSQL
2. Login to PostgreSQL and create an empty schema in the database
3. Use the ```OMOP CDM ddl - PostgreSQL.sql``` to create tables and fields into the schema for the CDM
4. Load data into the schema by modifying the ```OMOP CDM vocabulary load - PostgreSQL.sql``` script in the VocabImport folder to accept the correct tables.
5. Add constraints including primary and foreign keys by running ```OMOP CDM constraints - PostgreSQL.sql```
6. Add a minimum set of indexes to the data by running ```OMOP CDM indexes required - PostgreSQL.sql```

The database import mail fail due to incorrect data types. For my project I changed the database scheme to allow varchar, however I would highly suggest modifying the data itself to remove the non-numeric charachters as an easier solution.
