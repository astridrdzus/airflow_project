from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
#from hello import hello


#from credit_card_etl.py import extract_validate_load


def hello():
   print("hello world")

with DAG("pipe", start_date= datetime(2022,2,10),
    schedule_interval= "@daily", catchup=False) as dag:
    dummy= PythonOperator(
        task_id ="print_hello",
        python_callable =  hello,
    )

dummy

