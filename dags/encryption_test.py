from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from valkey.valkey_utils_plugin import ValkeyConnector
from aes_cbc_plugin.cbc_logic import AESCrypto
from aes_gcm_plugin.gcm_logic import AESGCMCrypto

def initialize():
    plaintext = "hello world"
    method = "gcm"
    print("Original:", plaintext)
    print("Method:", method)
    ValkeyConnector.push(
        "text",
        plaintext
    )
    ValkeyConnector.push(
        "method",
        method
    )

def decide_branch():

    value = ValkeyConnector.pull("method")
    print("Method:", value)

    if value.upper() == "GCM":
        print("Using GCM encryption")
        return "encrypt_gcm"
    else:
        print("Using CBC encryption")
        return "encrypt_cbc"


def encrypt_task_gcm():

    print("Starting GCM encryption")
    text = ValkeyConnector.pull("text")
    print("Received:", text)
   
    encrypted_text = AESGCMCrypto.encrypt(text)
    print("Encrypted:", encrypted_text)
    ValkeyConnector.push(
        "text",
        encrypted_text
    )


def decrypt_task_gcm():

    print("Starting GCM decryption")
    encrypted_text = ValkeyConnector.pull("text")
    print("Received:", encrypted_text)
    
    decrypted_text = AESGCMCrypto.decrypt(encrypted_text)
    print("Decrypted:", decrypted_text)
    ValkeyConnector.push(
        "text",
        decrypted_text
    )

def encrypt_task_cbc():

    print("Starting CBC encryption")
    text = ValkeyConnector.pull("text")
    print("Received:", text)
    
    encrypted_text = AESCrypto.encrypt(text)
    print("Encrypted:", encrypted_text)
    ValkeyConnector.push(
        "text",
        encrypted_text
    )


def decrypt_task_cbc():

    print("Starting CBC decryption")
    encrypted_text = ValkeyConnector.pull("text")
    print("Received:", encrypted_text)
    
    decrypted_text = AESCrypto.decrypt(encrypted_text)
    print("Decrypted:", decrypted_text)
    ValkeyConnector.push(
        "text",
        decrypted_text
    )


def cleanup():

    ValkeyConnector.delete("text")


with DAG(
    dag_id="encryption_test",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="initialize",
        python_callable=initialize
    )

    branch = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=decide_branch
    )
    task2 = PythonOperator(
        task_id="encrypt_cbc",
        python_callable=encrypt_task_cbc
    )

    task3 = PythonOperator(
        task_id="decrypt_cbc",
        python_callable=decrypt_task_cbc
    )

    task4 = PythonOperator(
        task_id="encrypt_gcm",
        python_callable=encrypt_task_gcm
    )

    task5 = PythonOperator(
        task_id="decrypt_gcm",
        python_callable=decrypt_task_gcm
    )


    task6 = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        trigger_rule="none_failed_min_one_success"
    )

    task1 >> branch

    branch >> task2 >> task3 >> task6
    branch >> task4 >> task5 >> task6