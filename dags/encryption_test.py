from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from valkey.valkey_utils_plugin import ValkeyConnector
from aes_cbc_plugin.cbc_logic import AESCrypto

def encrypt_task():

    text = "hello world"
    
    encrypted_text = AESCrypto.encrypt(text)
    print("Encrypted:", encrypted_text)
    ValkeyConnector.push(
        "text",
        encrypted_text
    )


def decrypt_task():

    encrypted_text = ValkeyConnector.pull("text")
    print("Received:", encrypted_text)
    
    decrypted_text = AESCrypto.decrypt(encrypted_text)
    print("Decrypted:", decrypted_text)


def cleanup():

    ValkeyConnector.delete("text")


with DAG(
    dag_id="encryption_test",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="encrypt",
        python_callable=encrypt_task
    )

    task2 = PythonOperator(
        task_id="decrypt",
        python_callable=decrypt_task
    )

    task3 = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup
    )

    task1 >> task2 >> task3