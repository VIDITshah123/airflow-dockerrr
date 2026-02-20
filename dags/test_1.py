from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def email_test(**kwargs):
    try:
        EmailOperator(
            task_id="send_test_email",
            to=['smtptesting46@gmail.com'],
            from_email="soham.rane@avenues.info",
            subject="SMTP Test",
            html_content="""
                <h1 style="color:blue;">Hello from Airflow vidit</h1>
                <p>This is a <b>HTML email</b>.</p>
                <p style="color:red;">SMTP test successful.</p>
                """,
            conn_id="smtp_connection"
        ).execute(context=kwargs)

        print(f"email sent successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")



with DAG(
    dag_id="smtp_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    send_test_email = PythonOperator(
        task_id = 'test_email',
        python_callable = email_test
    )
