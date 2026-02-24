from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from logicc import DBConnector

def plugin_test():
    db = DBConnector()

# query1
    result1 = db.select_statement(
    schema_name="file_sys",
    table_name="file_data",
    selector="SELECT",
    update_values="",
    column_name="*",
    condition="is_expired = 'T'"
    )
    print("Result 1:", result1)
    print("-----------------------")
    
# query2
    result2 = db.select_statement(
    selector="SELECT",
    column_name="file_id, file_name",
    condition="file_id > 10"
    )
    print("Result 2:", result2)
    print("------------------------")

# query3
    result3 = db.select_statement(
    selector="SELECT",
    column_name="*",
    condition=None
    )
    print("Result 3:", result3)

# query4
    result4 = db.select_statement(
    selector="SELECT",
    column_name="file_name",
    condition="1=1 LIMIT 5"
    )
    print("Result 4:", result4)
    print("------------------------")

# query5
    result5 = db.select_statement(
    selector="UPDATE",
    update_values="is_expired = 'F'",
    condition="file_id = 1"
    )
    print("Rows Updated:", result5)
    print("------------------------")

# query6
    result6 = db.select_statement(
    selector="UPDATE",
    update_values="is_expired = 'T'",
    condition="file_id > 50"
    )
    print("Rows Updated:", result6)
    print("------------------------")

# query7
    result7 = db.select_statement(
    selector="DELETE",
    column_name="",
    condition="file_id = 999"
    )
    print("Rows Deleted:", result7)
    print("------------------------")

# query8
    result8 = db.select_statement(
    selector="DELETE",
    condition="is_expired = 'T'"
    )
    print("Rows Deleted:", result8)
    print("------------------------")

# query9
    result9 = db.select_statement(
    selector="SELECT",
    column_name="file_id, created_date",
    condition="is_expired = 'F' AND file_id < 100"
    )
    print("Result 9:", result9)
    print("------------------------")

# query10
    result10 = db.select_statement(
    selector="SELECT",
    column_name="COUNT(*)",
    condition=None
    )
    print("Result 10:", result10)
    print("------------------------")

with DAG(
    dag_id="plugin_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    plugin_test_task = PythonOperator(
        task_id = 'test_email',
        python_callable = plugin_test
    )