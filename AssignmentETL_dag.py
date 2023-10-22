from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Gabriel_Huga',
    'start_date': datetime(2023, 10, 20, 4, 0, 0),
}

connection_dag = DAG(
    dag_id='airflow_openaq_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust as needed
    catchup=False,
    tags=['openaq'],
)

def get_openaq_data():
    url = "https://api.openaq.org/v2/locations?limit=100&page=1&offset=0&sort=desc&radius=1000&country=ID&location=Jakarta%20Central&order_by=lastUpdated&dump_raw=false"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    logging.info("OpenAQ API Response: %s", response.text)
    
    return response.json()

def transform_and_store_data(**kwargs):
    response = kwargs['ti'].xcom_pull(task_ids='get_openaq_data')
    
    if not response:
        return

    jakarta_data = [item for item in response.get('results', []) if item.get('country') == 'ID' and item.get('location') == 'Jakarta Central']
    if not jakarta_data:
        return

    pg_hook = PostgresHook(postgres_conn_id='assignment_docker')

    insert_sql = """
    INSERT INTO assignment_table (parameter, value, timestamp)
    VALUES (%s, %s, %s)
    """
    
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for item in jakarta_data:
        parameter = item['parameter']
        value = item['value']
        timestamp = item['date']['utc']

        cursor.execute(insert_sql, (parameter, value, timestamp))

    connection.commit()
    cursor.close()

get_data_task = PythonOperator(
    task_id='get_openaq_data',
    python_callable=get_openaq_data,
    provide_context=True,
    dag=connection_dag,
)

transform_and_store_data_task = PythonOperator(
    task_id='transform_and_store_data',
    python_callable=transform_and_store_data,
    provide_context=True,
    dag=connection_dag,
)

get_data_task >> transform_and_store_data_task
