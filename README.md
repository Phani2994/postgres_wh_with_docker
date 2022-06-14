# postgres_wh_with_docker
Deploying a Postgres database using docker-compose along with airflow

In this project, we are aiming to first deploy a Postgres database using docker-compose along with airflow as a local deployment. 
Our next goal is to access sample data present in some public CSV files in a AWS S3 bucket, process them and store the processed data in our postgres database.
The pipeline is deployed as an airflow dag - "shopify_data_daily", which pulls the data from the remote CSV files. After processing the data for each date ("extracted_date" field
in the data), the program connects with the postgres database and inserts the data in to it performing both before and after checks on the data in the postgres database.

NOTE: In this project deployment you would see that the job would FAIL, because in our list of files, we have a file which is not accessible ( does not have public access).
This was intentional, because in the production deployment, we would want the job to fail and alert us if there is a issue while processing the data. 
Still the program will porcess all the good files and good data. Only the files (or data with issue) will be unprocessed and should be handled accordingly.

First let's start with local deployment and access the airflow UI to start the job.

## Informations

* Based on official [Airflow](https://github.com/apache/airflow/blob/main/docs/apache-airflow/start/docker-compose.yaml) image(apache/airflow:2.2.0) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/apache-airflow)

## Initialize the database

On all operating systems, you need to run database migrations and create the first user account. To do it, run.

    docker-compose up airflow-init

After initialization is completed, you can access the account created using login airflow and the password airflow.

## Running Airflow

Start all the services, including the postgres warehouse database as per the configurations made in the docker-composer.yml file:

    docker-compose up


## Accessing the Airflow web interface and postgres database
 
Once the cluster has started up, you can log in to the web interface and try to run some tasks.

The airflow webserver is available at: http://localhost:8080. The default account has the login airflow and the password airflow

The postgres database could be accessed via any tool like pdadmin or DBeaver using the following credentials:
      - POSTGRES_USER=algolia_user
      - POSTGRES_PASSWORD=algolia_pwd
      - POSTGRES_DB=algolia_wh
      - port = 5432

## Usage

Once the airflow UI is accessed, we can start the dag - "shopify_data_daily". Once the job is successfully completed, we can access the data from postgres database.

You would see that the job would FAIL, because in our list of files, we have a file which is not accessible ( does not have public access).

Also, in this example we are passing dates for which we need to processs as static parameters as per the scope of the aim. But we could pass date as a parameter, by running airflow dag with configs. When we deploy this pipeline as a daily job, we could use {{ ds }} to to execute the job dynamically everyday.
This way we could also use the same program for backfilling the data for the dates where it had issues.


## Cleaning up

To stop and delete containers,:

    docker-compose down

To stop and delete containers, delete volumes with database data and download images, run:

    docker-compose down --volumes --rmi all
