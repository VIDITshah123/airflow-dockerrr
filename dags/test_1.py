from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from logicc import DBConnector

def plugin_test():
    db = DBConnector()
    result = db.select_statement(
        schema_name="file_sys",
        table_name="file_data",
        column_name="*",
        condition="is_expired = 'T'"
    )

    print("Query Result 1:", result)
    print("plugin test successful")


with DAG(
    dag_id="smtp_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    plugin_test_task = PythonOperator(
        task_id = 'test_email',
        python_callable = plugin_test
    )