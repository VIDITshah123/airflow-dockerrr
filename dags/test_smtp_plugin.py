# test_smtp_plugin
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from email_logic import SMTPConnector

# Default arguments for the DAG
default_args = {
    'owner': 'Vidit',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 17),
}

def mail_test():
    mail = SMTPConnector()

    #1st mail
    mail.send_email(
        subject="Test Email1", 
        body="<h3>Connection Verified! for 1</h3>", 
        send_to="smtptesting46@gmail.com"
    )
    print("1st mail sent")

    #2nd mail
    mail.send_email(
        subject="Test Email2", 
        body="<h3>Connection Verified! for 2</h3>", 
        send_to="smtptesting46@gmail.com",
        files_path=["/opt/airflow/delete/1.txt"]
    )
    print("2nd mail sent")

    #3
    mail.send_email(
        subject="Test Email2", 
        body="<h3>Connection Verified! for 2</h3>", 
        send_to="smtptesting46@gmail.com",
        files_path=["/opt/airflow/delete/3.md"]
    )
    print("3rd mail sent")

    #4
    mail.send_email(
        subject="Test Email2", 
        body="<h3>Connection Verified! for 2</h3>", 
        send_to="smtptesting46@gmail.com",
        files_path=["/opt/airflow/delete/4.md","/opt/airflow/delete/5.md","/opt/airflow/delete/2.txt"]
    )
    print("4th mail sent")

with DAG(
    dag_id='test_smtp_plugin',
    default_args=default_args,
    description='Testing Gmail SMTP with manual from_email in code',
    schedule=None,  # Manual trigger
    catchup=False,
) as dag:

    send_test_email = PythonOperator(
        task_id='email_task',
        python_callable=mail_test
    )
    
    send_test_email