
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models.variable import Variable

import logging
from box import Box
from src.etl_data import extract, transform, load
from src.ml_process import MLprocess
from src.text_process import tokenize_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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


def run_ml(**context):
    ml_model = MLprocess(context["params"]["config_spark"],
                         context["params"]["config_db"],
                         context["params"]["config_analysis"])
    df = ml_model.download_input()
    df = ml_model.predict_model(df)
    ml_model.upload_output(df)


def postprocess(**context):
    tokenize_text(context["params"]["config_spark"],
                  context["params"]["config_db"],
                  context["params"]["config_analysis"])



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
    task_id='run_dbt',
    bash_command=f'cd {config.dbt.path} && dbt run --full-refresh && dbt docs generate',
    dag=dag
)


# ML
run_ml_pipeline = PythonOperator(
    task_id = 'run_ml',
    #python_callable param points to the function you want to run 
    python_callable = run_ml,
    params = {
        "config_spark": config.spark,
        "config_db": config.db,
        "config_analysis": config.analysis
    },
    #dag param points to the DAG that this task is a part of
    dag = dag)


# postprocessing
run_postprocessing_pipeline = BashOperator(
    task_id = 'postprocessing',
    bash_command='python3 /var/lib/airflow/dags/postprocess.py',
    dag=dag
)



# Assign the order of the tasks in our DAG
run_etl_pipeline >> run_elt_pipeline >> run_ml_pipeline >> run_postprocessing_pipeline
