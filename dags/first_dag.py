from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Define the function the task will execute
def print_hello():
    print("Hello World")

# 2. Initialize the DAG
with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False
) as dag:

    # 3. Define the Task
    hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello
    )

    # Note: Since there's only one task, no dependencies (>>) are needed.
    hello_task