from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import psycopg2


date_created = 28
is_expired = 'Y'
delete_flag = 'N'
zip_flag = 'N'
deletion_attempted = 'Y'


def fetch_not_expired_files():
    print("Task started: Fetching not expired files...")

    conn = BaseHook.get_connection('postgres_airflow')  # use your connection id

    connection = psycopg2.connect(
    host=conn.host,
    port=conn.port,
    dbname=conn.schema,   # must be airflow
    user=conn.login,
    password=conn.password
)

    cursor = connection.cursor()

    sql = """
            SELECT *
            FROM file_sys.file_data
            WHERE is_expired = 'F';
        """

    cursor.execute(sql)
    print("Connected Database:", conn.schema)

    records = cursor.fetchall()
    cursor.close()
    connection.close()
    
    print("====================================")
    print(f"records type:{type(records)}")
    print("====================================")
    print(f"records:\n{records}")
    print("====================================")
    print("Fetched Records:")
    for row in records:
        print(row)
    print("====================================")

    if not records:
        print("No non-expired files found.")
    else:
        print(f"Total files fetched: {len(records)}")
        for row in records:
            print(f"File ID: {row[0]}, Path: {row[1]}, Expired: {row[3]}")

    print("Task completed successfully.")
    return "done"


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



def check_and_update_expired_files():
    """
    Extract files with is_expired='F', check if date is more than 30 days old,
    and update is_expired field to 'T' for expired files.
    """
    print("Task started: Check and update expired files...")
    
    conn = BaseHook.get_connection('postgres_airflow')
    
    connection = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password
    )
    
    cursor = connection.cursor()
    
    try:
        # Fetch files where is_expired = 'F'
        sql = """
            SELECT file_id, date_created, is_expired
            FROM file_sys.file_data
            WHERE is_expired = 'F';
        """
        
        cursor.execute(sql)
        records = cursor.fetchall()
        
        print(f"Connected to database: {conn.schema}")
        print(f"Fetched {len(records)} non-expired files")
        
        if not records:
            print("No non-expired files found.")
            return "no_files"
        
        # Calculate threshold date (30 days ago from current date)
        current_date = datetime.now()
        threshold_date = current_date - timedelta(days=30)
        
        expired_files = []
        
        for record in records:
            file_id, date_created, is_expired = record
            
            print(f"Processing file_id: {file_id}, date_created: {date_created}")
            
            # Check if file is older than 30 days
            if date_created < threshold_date:
                print(f"File {file_id} is expired (older than 30 days)")
                expired_files.append(file_id)
                
                # Update is_expired to 'T'
                update_sql = """
                    UPDATE file_sys.file_data
                    SET is_expired = 'T'
                    WHERE file_id = %s;
                """
                
                cursor.execute(update_sql, (file_id,))
                print(f"Updated file_id {file_id}: is_expired set to 'T'")
            else:
                print(f"File {file_id} is not expired (within 30 days)")
        
        # Commit the changes
        connection.commit()
        
        print(f"Successfully updated {len(expired_files)} files as expired")
        print(f"Expired file IDs: {expired_files}")
        
        return f"updated_{len(expired_files)}_files"
        
    except Exception as e:
        print(f"Error in check_and_update_expired_files: {e}")
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()


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
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag: 

    # fetch_files_task = PythonOperator(
    #     task_id='fetch_not_expired_files',
    #     python_callable=fetch_not_expired_files
    # )
    
    check_update_expired_task = PythonOperator(
        task_id='check_and_update_expired_files',
        python_callable=check_and_update_expired_files
    )
    
    expiration_check_task = PythonOperator(
        task_id='check_expiration_flag_task',
        python_callable=check_expiration_flag
    )
    
    delete_task = PythonOperator(
        task_id='delete_task',
        python_callable=delete_task
    )
    
    # fetch_files_task >> check_update_expired_task >> expiration_check_task >> delete_task
    check_update_expired_task >> expiration_check_task >> delete_task