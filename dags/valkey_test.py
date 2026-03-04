from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from valkey.valkey_utils_plugin import ValkeyConnector


def sql_task():

    query_result = "hello world"

    ValkeyConnector.push(
        "sql_result",
        query_result
    )


def process_task():

    data = ValkeyConnector.pull("sql_result")

    print("Received:", data)


def cleanup():

    ValkeyConnector.delete("sql_result")


with DAG(
    dag_id="valkey_test",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="sql_query",
        python_callable=sql_task
    )

    task2 = PythonOperator(
        task_id="process",
        python_callable=process_task
    )

    task3 = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup
    )

    task1 >> task2 >> task3