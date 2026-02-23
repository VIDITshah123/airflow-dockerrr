from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import psycopg2
import os

def run_sql_script():
    try:
        print("Trying to establish connection to database...")
        conn = BaseHook.get_connection('postgres_airflow')
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password
        )
        print("Connection established successfully.")
    except Exception as e:
        print(f"Error while connecting to database: {e}")
        raise e

    cursor = connection.cursor()

    try:
        print("Running sql script...")
        update_sql = """
        UPDATE file_sys.file_data
        SET is_expired = 'F', deletion_attempted = 'F'
        WHERE (is_expired = 'T' AND deletion_attempted = 'F') OR (is_expired = 'T' AND deletion_attempted = 'T');
        """
        cursor.execute(update_sql)
        connection.commit()
        print("Sql script executed successfully.")
    except Exception as e:
        print(f"Error while running sql script: {e}")
    finally:
        cursor.close()
        connection.close()
    print("Sql script executed successfully.")

def create_files():
    return

with DAG(
    dag_id='create_test_data_dag',    
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag: 
    run_sql_script_task = PythonOperator(
        task_id='run_sql_script',
        python_callable=run_sql_script
    )

    create_files_task = PythonOperator(
        task_id='create_files',
        python_callable=create_files
    )

    run_sql_script_task >> create_files_task