from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from credit_card_etl import extract_validate_load
##openpyxl



with DAG("credit_card_dag", start_date=datetime(2021,1,1),
    schedule_interval="@daily", catchup=False) as dag:

    etl = PythonOperator(
        task_id= "etl",
        python_callable = extract_validate_load,
        
    )


etl