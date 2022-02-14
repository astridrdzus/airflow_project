
#Created by Astrid Rodriguez
#Creation date: Feb 12th 2022

import sqlite3
from sqlite3 import dbapi2
import pandas as pd
from sqlalchemy import create_engine

#---create database----#
engine = create_engine('sqlite:///credit_card.sqlite', echo = True)

def extract_validate(path):
    #-----Extract----------#
    df = pd.read_excel(path)

    #-----Validations------#
    #Check if dataframe is empty
    if df.empty:
        print("0 rows read. Finishng execution")
        return False
    print(df)

    #Primary Key validation
    if pd.Series(df['customer_id']).is_unique:
        pass
    else:
        raise Exception("Duplicate Primary Key found")

    # Nulls validation
    if df.isnull().values.any():
        raise Exception("Null values found")
    return df



def load_app_table(df):   
    conn = sqlite3.connect('credit_card.sqlite')
    cursor =  conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS app_data(
            customer_id INT(20) ,
            customer_name VARCHAR(200),
            click_events INT(15),
            date DATE,
            CONSTRAINT primary_key_contraint PRIMARY KEY (customer_id)
        )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df.to_sql("app_data", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close databse successfully")

def load_customer_table(df):   
    conn = sqlite3.connect('credit_card.sqlite')
    cursor =  conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS customer_data(
            customer_id INT(20) ,
            customer_name VARCHAR(200),
            customer_email VARCHAR(200),
            customer_address VARCHAR(300),
            date DATE,
            CONSTRAINT primary_key_contraint PRIMARY KEY (customer_id)
        )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df.to_sql("app_data", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close databse successfully")

def load_riskmodel_table(df):   
    conn = sqlite3.connect('credit_card.sqlite')
    cursor =  conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS riskmodel_data(
            customer_id INT(20) ,
            customer_name VARCHAR(200),
            customer_rfc VARCHAR(200),
            CONSTRAINT primary_key_contraint PRIMARY KEY (customer_id)
        )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df.to_sql("app_data", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close databse successfully")

def load_riskmodel_table(df):   
    conn = sqlite3.connect('credit_card.sqlite')
    cursor =  conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS riskmodel_data(
            customer_id INT(20) ,
            customer_swipes INT(20),
            customer_credit_line INT(20),
            customer_last_purchase, 
            CONSTRAINT primary_key_contraint PRIMARY KEY (customer_id)
        )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df.to_sql("app_data", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close databse successfully")


app = extract_validate("app_data.xlsx")
customer = extract_validate("customers_data.xlsx")
model = extract_validate("risk_model_data.xlsx")
payments = extract_validate("payments_data.xlsx")
