from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello World")
    a = 1
    b = 2
    c = a + b
    print(f"Sum of {a} and {b} is {c}")
    print(f"Type of c is {type(c)}")
    print("Dag is running successfully!")

with DAG(
    dag_id='first_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello
    )
    
    hello_task