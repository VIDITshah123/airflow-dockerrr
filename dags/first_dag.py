from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

date_created = 33
is_expired = 'Y'
delete_flag = 'Y'
zip_flag = 'N'
deletion_attempted = 'N'

def check_expiration_flag():
    try:
        if date_created < 30:
            print("Date created is less than 30 days")
            is_expired = 'N'
            print(f"is_expired: {is_expired}")
            print("is_expired checked successfully")
        else:
            print("Date created is greater than 30 days")
            is_expired = 'Y'
            print(f"is_expired: {is_expired}")
            print("is_expired checked successfully")
    except Exception as e:
        print(f"Error while checking flag for expiration: {e}")
        is_expired = 'N'
        print(f"is_expired: {is_expired}")
        print("is_expired checked successfully")



def delete_task():
    if is_expired == 'Y':
        try:    
            # first check deletion_attempted flag
            if deletion_attempted == 'N':
                print("Deleting task")
            else:
                print("Deletion already attempted")
        except Exception as e:
            print(f"Error while deleting the file: {e}")
            deletion_attempted = 'N'

        # if delete flag is Y and zip flag is N
        if delete_flag == 'Y' and zip_flag == 'N' and deletion_attempted == 'N':
            try:
                print("Delete flag is Y and zip flag is N")
                print("Deleting the file")
                # set deletion_attempted to 'Y'
                deletion_attempted = 'Y'
                print("Deletion attempted")
            except Exception as e:
                print(f"Error while deleting the file: {e}")
                deletion_attempted = 'N'
                print("Deletion failed")
        # if delete flag is N and zip flag is Y
        elif delete_flag == 'N' and zip_flag == 'Y' and deletion_attempted == 'N':
            try:
                print("Delete flag is N and zip flag is Y")
                print("Zipping the file")
                # set deletion_attempted to 'Y'
                deletion_attempted = 'Y'
                print("Zipping attempted")
            except Exception as e:
                print(f"Error while zipping the file: {e}")
                deletion_attempted = 'N'
                print("Zipping failed")
        # if both delete and zip flags are N
        elif delete_flag == 'N' and zip_flag == 'N' and deletion_attempted == 'N':
            try:
                print("Delete flag is N and zip flag is N")
                # set deletion_attempted to 'N'
                deletion_attempted = 'N'
            except Exception as e:
                print(f"Error while handling the file: {e}")
                deletion_attempted = 'N'
        else:
            print("Something went wrong")
        
    else:
        print("Not expired, not deleting")



with DAG(
    dag_id='first_dag',    
    schedule=None,
) as dag: 
    
    expiration_check_task = PythonOperator(
        task_id='check_expiration_flag_task',
        python_callable=check_expiration_flag
    )
    delete_task = PythonOperator(
        task_id='delete_task',
        python_callable=delete_task
    )
    expiration_check_task >> delete_task