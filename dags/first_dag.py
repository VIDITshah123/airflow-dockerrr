from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import psycopg2
import os
import zipfile

def zip_file(source_path):
    DEST_PATH = "/opt/airflow/delete" 
    zip_name = os.path.basename(source_path) + ".zip"
    zip_path = os.path.join(DEST_PATH, zip_name)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(source_path, os.path.basename(source_path))

    return zip_path


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


def attemp_deletion():
    """
    Extract files with is_expired='T'& deletion_attempted = 'F'
    and performs updation or deletion for files that need deletion.
    """
    print("Task started: Attempt deletion...")
    
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
            SELECT file_id, file_path, is_expired, delete_flag, zip_flag, deletion_attempted
            FROM file_sys.file_data
            WHERE is_expired = 'T' AND deletion_attempted = 'F';
        """
        
        cursor.execute(sql)
        records = cursor.fetchall()
        
        print(f"Connected to database: {conn.schema}")
        print(f"Fetched {len(records)} non-expired files")
        
        if not records:
            print("No non-expired files found.")
            return "no_files"


        # Process each file
        for record in records:
            file_id, file_path, is_expired, delete_flag, zip_flag, deletion_attempted = record
            
            print(f"Processing file_id: {file_id}\nfile_path: {file_path}\nis_expired: {is_expired}\ndelete_flag: {delete_flag}\nzip_flag: {zip_flag}\ndeletion_attempted: {deletion_attempted}")
            print(f"record:\n {record}")

            # if delete flag is T and zip flag is F
            if delete_flag == 'T' and zip_flag == 'F':
                print(f"Delete flag is T and zip flag is F for file_id: {file_id}")
                print(f"Deleting the file at {file_path}")
                try:
                    os.remove(file_path)
                    update_sql = """
                    UPDATE file_sys.file_data
                    SET deletion_attempted = 'T'
                    WHERE file_id = %s;
                    """
                    cursor.execute(update_sql, (file_id,))
                    connection.commit()
                    print(f"Deletion successful for file_id: {file_id}")
                except Exception as e:
                    print(f"Error while deleting the file at file_id: {file_id}\nerror: {e}")
                    update_sql = """
                    UPDATE file_sys.file_data
                    SET deletion_attempted = 'F'
                    WHERE file_id = %s;
                    """
                    cursor.execute(update_sql, (file_id,))
                    connection.commit()
                    print(f"Deletion failed for file_id: {file_id}")



            # if delete flag is F and zip flag is T
            elif delete_flag == 'F' and zip_flag == 'T':
                print(f"Delete flag is F and zip flag is T for file_id: {file_id}")
                print(f"Zipping the file at {file_path}")
                try:
                    print(f"Zipping the file at {file_path}...\n")
                    zip_file(file_path)
                    update_sql = """
                    UPDATE file_sys.file_data
                    SET deletion_attempted = 'T'
                    WHERE file_id = %s;
                    """
                    cursor.execute(update_sql, (file_id,))
                    connection.commit()
                    print(f"Zipping successful for file_id: {file_id}")
                except Exception as e:
                    print(f"Error while zipping the file at file_id: {file_id}\nerror: {e}")
                    update_sql = """
                    UPDATE file_sys.file_data
                    SET deletion_attempted = 'F'
                    WHERE file_id = %s;
                    """
                    cursor.execute(update_sql, (file_id,))
                    connection.commit()
                    print(f"Zipping failed for file_id: {file_id}")



            # if both delete and zip flags are F or both are T
            elif delete_flag == zip_flag == 'F' or delete_flag == zip_flag == 'T':
                try:
                    print(f"Delete flag is F and zip flag is F or both are T pls check the database for file_id: {file_id}")
                    
                    update_sql = """
                        UPDATE file_sys.file_data
                        SET deletion_attempted = 'F'
                        WHERE file_id = %s;
                        """
                    cursor.execute(update_sql, (file_id,))
                    connection.commit()
                except Exception as e:
                    print(f"Error while handling the file at file_id: {file_id} \nerror: {e}")
                    update_sql = """
                        UPDATE file_sys.file_data
                        SET deletion_attempted = 'F'
                        WHERE file_id = %s;
                        """
                    cursor.execute(update_sql, (file_id,))
                    connection.commit()
            
            
            
            # else:
            #     print(f"Something went wrong or deletion already attempted, deletion_attempted: {deletion_attempted} \nfor file_id: {file_id}")
            
            
            print(f"Updated file_id {file_id}")
            print("----------------------------------------------------------------")
        # connection.commit()
        print("All files processed successfully.")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback()
        raise e
        
    finally:
        cursor.close()
        connection.close()
        print("Database connection closed.")



with DAG(
    dag_id='first_dag',    
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag: 
    check_update_expired_task = PythonOperator(
        task_id='check_and_update_expired_files',
        python_callable=check_and_update_expired_files
    )

    deletion_check_task = PythonOperator(
        task_id='attemp_deletion',
        python_callable=attemp_deletion
    )

    check_update_expired_task >> deletion_check_task