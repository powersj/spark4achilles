# spark4achilles

## Objective
Runs variations of ACHILLES analytics using Scala + Spark

## Data
Data was generated using the ETL-CMS repo and then loaded into PostgreSQL using the CommonDataModel repo. High-level instructions are below.

### Obtain Raw Data

To get the data:

1. Clone the ETL-CMS tool from: https://github.com/OHDSI/ETL-CMS/tree/unm-improvements
2. Change to the scripts directory
3. Run the ```get_synpuf_files.py``` script
4. The data will then be downloaded
5. Change to the python_etl directory
6. Follow the readme in this direcotry to setup your system.
7. Finally, run the ```CMS_SynPUF_ETL_CDM_v5.py``` script to convert

This will produce valid .csv files ready for import and usage.

### Importing to PostgreSQL

To add the raw data to PostgreSQL

1. Clone the CommonDataModel repo from: https://github.com/OHDSI/CommonDataModel/tree/master/PostgreSQL
2. Login to PostgreSQL and create an empty schema in the database
3. Use the ```OMOP CDM ddl - PostgreSQL.sql``` to create tables and fields into the schema for the CDM
4. Load data into the schema by modifying the ```OMOP CDM vocabulary load - PostgreSQL.sql``` script in the VocabImport folder to accept the correct tables.
5. Add constraints including primary and foreign keys by running ```OMOP CDM constraints - PostgreSQL.sql```
6. Add a minimum set of indexes to the data by running ```OMOP CDM indexes required - PostgreSQL.sql```

The database import mail fail due to incorrect data types. For my project I changed the database scheme to allow varchar, however I would highly suggest modifying the data itself to remove the non-numeric charachters as an easier solution.

## Environment
The author used two separate environments:

* Local Baremetal
  * Intel Core i5-3570K processor with 16 GB of memory, a 512 GB SSD, and runs Ubuntu 15.10.
  * Ran the ACHILLES benchmark and acted as a "single-node" Spark cluster

* Amazon Web Services (AWS) Elastic MapReduce (EMR)
  * Four memory-optimized compute nodes (r3.xlarge)
  * A fifth node acted as the master node
  * 4 vCPUs, 30 GB of memory, and an 80 GB SSD
  * EMR version 4.5.0

Both infrastructures used Apache Spark 1.6.1 and Apache Hadoop 2.7.2. The Scala-based Spark application will use OpenJDK 7, SBT 0.13.8, and Scala 2.10.6.

## How to Run
First, the data needs to be generated. See the readme in the data directory.

Next, sbt will bring in everything you need, but you need to build the JAR:
```bash
sbt assembly
```

Then to run with Spark:
```bash
spark-submit --class edu.gatech.cse8803.main.Main cse8803_project-assembly-1.3.jar
```

## Supplemental Material
 * Video: https://www.youtube.com/watch?v=k5bl7VhgEmQ
 * Paper: https://github.com/powersj/spark4achilles/blob/master/CSE8803_BDAH_2016.pdf

## Contributors
* [Joshua Powers](http://powersj.github.io/)
  * CSE8803 Big Data Analytics for Health Care  (Spring 2016)
  * Georgia Institute of Technology

## Reviewers
A huge thank you to the following for their feedback, evaluation, and support:
 * Dr. Jimeng Sun
 * The SunLab
 * Dr. Watler & Marjie Powers
 * Olga Martyusheva
 * Alex Balderson

# License
Apache 2.0 &copy; 2016 Joshua Powers
