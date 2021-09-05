# Data Pipelines with Airflow for Song Play Analysis

This project aims to orchestrate ETL from raw song and user data to Redshift cluster for later analysis. This is also used to satisfied with `Data Pipelines` project under [Data Engineer Nanodegree Program](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Prerequisite
- Python3
- Python virtual environment (aka `venv`)
- AWS credentials/config files under `~/.aws` directories (see more: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) This will be used to manipulate Redshift cluster and create necessary tables.

## Steps
1. In `devops` as working directory, bootstrap virtual environment with dependencies.
   ```bash
   $ python3 -m venv ./venv
   $ source ./venv/bin/activate
   $ pip install -r requirements.txt
   ```
2. Copy `template.etl.cfg` to `etl.cfg`.
   ```bash
   # assume `devops` as working directory
   $ cp ./template.etl.cfg ./etl.cfg
   ```
3. Fill `etl.cfg` on `CLUSTER` section. This section will be used to construct Redshift cluster from scratch. We are free to choose our values. Here are possible values.
   ```cfg
   [CLUSTER]
   DB_NAME=sample-data-pipeline-db
   DB_USER=sample-db-user
   DB_PASSWORD=<choose_whatever_you_want>
   DB_PORT=5439
   CLUSTER_TYPE=multi-node
   NUM_NODES=4
   NODE_TYPE=dc2.large
   CLUSTER_IDENTIFIER=sample-data-pipeline-cluster-identifier
   IAM_ROLE_NAME=sample-data-pipeline-iam-role-name
   ```
4. Spin up Redshift cluster. This script will omit Redshift cluster metadata related to public endpoint. In this case, for example, the endpoint is `sample-data-pipeline-cluster-identifier.cryhuvsimxxx.us-west-2.redshift.amazonaws.com`.
   ```bash
   # assume `devops` as working directory
   $ python spin_up_redshift_cluster.py
   ```
   ```log
   Successfully create cluster with metadata = RedshiftClusterMetadata(endpoint='sample-data-pipeline-cluster-identifier.cryhuvsimxxx.us-west-2.redshift.amazonaws.com', role_arn='arn:aws:iam::663276196999:role/sample-data-pipeline-iam-role-name', vpc_id='vpc-9817xxx')
   ```
5. Create necessary tables.
   ```bash
   # assume `devops` as working directory
   $ python create_tables.py
   ```
6. In root project directory, follow [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) until [Running Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#running-airflow) step.
7. Put credentials to access S3 and Redshift cluster as connections.
8. Look for `etl_dag` DAG and toggle it on to activate the DAG.
9. When finished using Redshift cluster, tear it down.
   ```bash
   # assume `devops` as working directory
   $ python tear_down_redshift_cluster.py
   ```
