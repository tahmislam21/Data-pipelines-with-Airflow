import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



########################################################
#
#   DAG Settings
#
#########################################################


default_args = {
    'owner': 'at3_bde',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email':[],
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id = 'at3',
    default_args=default_args,
    description='bde_3',
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################

AIRFLOW_DATA = "/home/airflow/gcs/data"
CENSUS_LGA = AIRFLOW_DATA+ "/Census LGA/"
LISTINGS = AIRFLOW_DATA +"/listings/"
NSW_LGA = AIRFLOW_DATA+"/NSW_LGA/"



#########################################################
#
#   Custom Logics for Operator
#
#########################################################

def load_raw_census_1(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    
   
    df = pd.read_csv(CENSUS_LGA+'2016Census_G01_NSW_LGA.csv')


    if len(df) > 0:
        column_names = list(df.columns)

        values = df[column_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = f"""INSERT INTO RAW.Census_G01_NSW_LGA({','.join(column_names)})
                        VALUES %s"""

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    
    else:
        None

    return None

def load_raw_census_2(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    
    df = pd.read_csv(CENSUS_LGA+'2016Census_G02_NSW_LGA.csv')

    if len(df) > 0:
        column_names = list(df.columns)

        values = df[column_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = f"""INSERT INTO RAW.Census_G02_NSW_LGA({','.join(column_names)})
                        VALUES %s"""

        results = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None




def load_raw_listings(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    combined_data = []

    for file in os.listdir(LISTINGS):
        if file.endswith(".csv"):
            file_path = os.path.join(LISTINGS, file)
            df = pd.read_csv(file_path)
            combined_data.append(df)

    final_df = pd.concat(combined_data, axis=0, ignore_index=True)

    if len(final_df) > 0:
        col_names = list(final_df.columns)

        values = final_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = f"""INSERT INTO RAW.listings({','.join(col_names)})
                    VALUES %s ON CONFLICT DO NOTHING"""  

        # Inserting in bathces
        batch_size = 1000  
        for i in range(0, len(values), batch_size):
            batch_values = values[i:i+batch_size]
            execute_values(conn_ps.cursor(), insert_sql, batch_values, page_size=len(batch_values))
        
        conn_ps.commit()

    return None


def load_raw_lga_suburb(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    df = pd.read_csv(NSW_LGA+'NSW_LGA_SUBURB.csv')

    if len(df) > 0:
        col_names = ['LGA_NAME', 'SUBURB_NAME']

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = f"""INSERT INTO RAW.NSW_LGA_SUBURB(LGA_NAME, SUBURB_NAME)
                    VALUES %s"""

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def load_raw_lga_code_name(**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    df = pd.read_csv(NSW_LGA+'NSW_LGA_CODE.csv')

    if len(df) > 0:
        col_names = list(df.columns)

        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = f"""INSERT INTO RAW.NSW_LGA_CODE({','.join(col_names)})
                    VALUES %s"""

        execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

    return None


#########################################################
#
#   Custom logics for  Operator
#
#########################################################


load_raw_census_g01 = PythonOperator(
    task_id="load_raw_census_1_id",
    python_callable=load_raw_census_1,
    op_kwargs={},
    provide_context=True,
    dag=dag
)
load_raw_census_g02 = PythonOperator(
    task_id="load_raw_census_2_id",
    python_callable=load_raw_census_2,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

load_raw_listings = PythonOperator(
    task_id="load_raw_listings_id",
    python_callable=load_raw_listings,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

load_raw_lga_suburb = PythonOperator(
    task_id="load_raw_lga_suburb_id",
    python_callable=load_raw_lga_suburb,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

load_raw_lga_code_name = PythonOperator(
    task_id="load_raw_lga_code_name_id",
    python_callable=load_raw_lga_code_name,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

#  Sequence of Running Custom Logics:
[load_raw_census_g01 ,load_raw_census_g02, load_raw_listings] >> load_raw_lga_suburb >> load_raw_lga_code_name
