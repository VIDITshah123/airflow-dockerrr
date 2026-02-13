from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def calculate(**context):
    a = context["params"]["a"]
    b = context["params"]["b"]

    # Access plugin macro
    result_add = context["macros"].MathFunctions.add(a, b)
    result_sub = context["macros"].MathFunctions.subtract(a, b)

    print(f"Addition Result: {result_add}")
    print(f"Subtraction Result: {result_sub}")


with DAG(
    dag_id="test_1",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={"a": 10, "b": 5},
) as dag:

    run_calculation = PythonOperator(
        task_id="run_calculation",
        python_callable=calculate,
        provide_context=True
    )
