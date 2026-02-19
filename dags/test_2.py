from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'Vidit',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 17),
}

def send_email_function():
    """Function to send email using EmailOperator"""
    email_operator = EmailOperator(
        task_id='send_email_operator',
        # Connection ID created in Airflow UI
        conn_id='gmail_smtp_conn', 
        # Manually defining the sender here
        from_email='smtptesting46@gmail.com', 
        to='smtptesting46@gmail.com',
        subject='Airflow SMTP Test: Manual From Address',
        html_content="""
            <h3>Connection Verified!</h3>
            <p>This email successfully bypassed the 'Extra' field JSON by 
            defining <b>from_email</b> directly in Python code.</p>
        """
    )
    return email_operator.execute(context={})

with DAG(
    dag_id='test_gmail_smtp_manual_config',
    default_args=default_args,
    description='Testing Gmail SMTP with manual from_email in code',
    schedule=None,  # Manual trigger
    catchup=False,
) as dag:

    send_test_email = PythonOperator(
        task_id='send_test_email_task',
        python_callable=send_email_function
    )
    
    send_test_email