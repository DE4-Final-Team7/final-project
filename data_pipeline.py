
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models.variable import Variable

import logging
from box import Box
from src.etl_data import extract, transform, load


logging.basicConfig(level = logging.INFO)


config = Box(Variable.get("config", deserialize_json=True))
config.api.video.params.key = Variable.get("api_key")
config.api.comment.params.key = Variable.get("api_key")
config.api.category.params.key = Variable.get("api_key")
db_info = Box(Variable.get("db_url_user_password", deserialize_json=True))
config.db.url = db_info.url
config.db.properties.user = db_info.user
config.db.properties.password = db_info.password



def run_etl(**context):
    extracted_data = extract(context["params"]["config_api"])
    transformed_data = transform(extracted_data, context["params"]["config_spark"])
    load(transformed_data, context["params"]["config_db"])



dag = DAG(
    dag_id='data-pipeline',
    start_date=datetime(2025, 1, 4),
    catchup=False,
    tags=['project'],
    schedule='0 * * * *')


# ETL
run_etl_pipeline = PythonOperator(
    task_id = 'run_etl',
    #python_callable param points to the function you want to run 
    python_callable = run_etl,
    params = {
        "config_api": config.api,
        "config_spark": config.spark,
        "config_db": config.db
    },
    #dag param points to the DAG that this task is a part of
    dag = dag)


# ELT
run_elt_pipeline = BashOperator(
    task_id='dbt_dev',
    bash_command=f'exit && cd {config.dbt.path} && dbt run --full-refresh && dbt docs generate',
    dag=dag
)


# Assign the order of the tasks in our DAG
run_etl_pipeline >> run_elt_pipeline











