# Data Pipelines using Airflow for Sparkify
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

We have decided to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. We have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Prerequisites to run the Project locally

1. Install Airflow and create variable AIRFLOW_HOME and AIRFLOW_CONFIG finally place dags and plugins on airflow_home directory.
2. Initialize Airflow database with `airflow initdb`, and open webserver with `airflow webserver`
3. Access the server on `http://localhost:8080`
4. Create the following connections by accessing Admin -> Connections -> Create: 

**AWS Connection**
* Conn Id: Enter aws_credentials.
* Conn Type: Enter Amazon Web Services.
* Login: Enter your Access key ID from the IAM User credentials.
* Password: Enter your Secret access key from the IAM User credentials.

**Redshift Connection**
* Conn Id: Enter redshift.
* Conn Type: Enter Postgres.
* Host: Enter the endpoint of our Redshift cluster, excluding the port at the end. 
* Schema: The Redshift database we want to connect to.
* Login: Enter awsuser.
* Password: Enter the password created when launching the Redshift cluster.
* Port: Enter 5439.

## Data Source 
* Log data: `s3://udacity-dend/log_data`
* Song data: `s3://udacity-dend/song_data`

## Project Strucute
* dags/sparkify_main_dag.py : Main DAG definition for running main tasks and task dependencies
* dags/sparkify_load_dim_subdag.py : SubDag for loading of Dimensional tables tasks
* plugins/helpers/sql_queries.py: Contains SQL statements for table data insertions
* plugins/operators/create_tables.sql: Contains SQL statements for table creation
* plugins/operators/create_tables.py: Operator for creating Staging, Fact and Dimentional tables
* plugins/operators/stage_redshift.py: Operator for copying data from S3 buckets to redshift staging tables
* plugins/operators/load_dimension.py: Operator for loading data from redshift staging tables into dimensional tables
* plugins/operators/load_fact.py: Operator for loading data from redshift staging tables into fact table
* plugins/operators/data_quality.py: Operator for validating data quality in redshift.

### Sparkify DAGs
DAG parameters:

* The DAG does not have dependencies on past runs
* DAG has schedule interval set to hourly
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Email are not sent on retry
* DAG contains default_args dict binded to the DAG

* Task dependencies are set as following:


### Operators
Operators create necessary tables, stage the data, transform the data, and run checks on data quality.

Connections and Hooks are configured using Airflow's built-in functionalities.

All of the operators and task run SQL statements against the Redshift database. 

#### Stage Operator
The stage operator loads any JSON and CSV formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

- **RedshiftStage operator**: There is a task to stage data from S3 to Redshift.

- **Params**: The task uses params to generate the copy statement dynamically. It also contains a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

#### Fact and Dimension Operators
The dimension and fact operators make use of the SQL helper class to run data transformations. Operators takes the SQL statement from the helper class and target the database on which to run the query against.

Dimension table load is done with the truncate-insert pattern where the target table is emptied before the load. There is a parameter that allows switching between insert modes when loading dimensions. Fact tables are massive so they only allow append type functionality.

- **Dimension load operator**: Dimensions are loaded using the LoadDimensionOperator operator

- **Fact load operator**: Facts are loaded using the LoadFactOperator operator

#### Data Quality Operator
The data quality operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result are checked and if there is no match, the operator raises an exception and the task is retried and fails eventually.

For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

- **Data quality operator**: Data quality check is done using DataQualityOperator and at least one data quality check is done and raises an error if the check fails

### Airflow UI views of DAG and plugins

The dag follows the data flow and all the tasks have a dependency and DAG begins with a start_execution task and ends with a end_execution task.