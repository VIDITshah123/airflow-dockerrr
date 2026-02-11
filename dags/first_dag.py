from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

date_created = 28
is_expired = 'Y'
delete_flag = 'N'
zip_flag = 'N'
deletion_attempted = 'Y'

def check_expiration_flag():
    global is_expired
    try:
        if date_created < 30:
            print("Date created is less than 30 days")
            is_expired = 'N'
            print(f"is_expired: {is_expired}")
            print("is_expired checked successfully & its N")
        else:
            print("Date created is greater than 30 days")
            is_expired = 'Y'
            print(f"is_expired: {is_expired}")
            print("is_expired checked successfully & its Y")
            return is_expired
    except Exception as e:
        print(f"Error while checking flag for expiration: {e}")
        is_expired = 'N'
        print(f"is_expired: {is_expired}")
        print("is_expired checked successfully & its N due to error")
        return is_expired



def delete_task():
    global deletion_attempted
    global is_expired
    print(f"is_expired: {is_expired}")
    print(f"deletion_attempted: {deletion_attempted}")
    if is_expired == 'Y':
        # first check deletion_attempted flag
        if deletion_attempted == 'N':
            print("calling delete task")

            # if delete flag is Y and zip flag is N
            if delete_flag == 'Y' and zip_flag == 'N':
                try:
                    print("Delete flag is Y and zip flag is N")
                    print("Deleting the file")
                    # set deletion_attempted to 'Y'
                    deletion_attempted = 'Y'
                    print(f"Deletion attempted, new value: {deletion_attempted}")
                except Exception as e:
                    print(f"Error while deleting the file: {e}")
                    deletion_attempted = 'N'
                    print(f"Deletion failed, new value: {deletion_attempted}")
            # if delete flag is N and zip flag is Y
            elif delete_flag == 'N' and zip_flag == 'Y':
                try:
                    print("Delete flag is N and zip flag is Y")
                    print("Zipping the file")
                    # set deletion_attempted to 'Y'
                    deletion_attempted = 'Y'
                    print(f"Zipping attempted, new value: {deletion_attempted}")
                except Exception as e:
                    print(f"Error while zipping the file: {e}")
                    deletion_attempted = 'N'
                    print(f"Zipping failed, new value: {deletion_attempted}")
            # if both delete and zip flags are N or both are Y
            elif delete_flag == zip_flag == 'N' or delete_flag == zip_flag == 'Y':
                try:
                    print("Delete flag is N and zip flag is N or both are Y pls check the database")
                    # set deletion_attempted to 'N'
                    deletion_attempted = 'N'
                    print(f"Handling completed, new value: {deletion_attempted}")
                except Exception as e:
                    print(f"Error while handling the file: {e}")
                    deletion_attempted = 'N'
                    print(f"Handling failed, new value: {deletion_attempted}")
            else:
                print(f"Something went wrong or deletion already attempted, the value is: {deletion_attempted}")
        else:
            print(f"Deletion already attempted, the value is: {deletion_attempted}")

    else:
        print(f"Not expired,\n not deleting value: {is_expired}")



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