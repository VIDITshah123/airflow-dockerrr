from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import psycopg2


# date_created = 28
# is_expired = 'Y'
# delete_flag = 'N'
# zip_flag = 'N'
# deletion_attempted = 'Y'


# def fetch_not_expired_files():

#     print("Task started: Fetching not expired files...")
#     conn = BaseHook.get_connection('postgres_airflow')
#     connection = psycopg2.connect(
#     host=conn.host,
#     port=conn.port,
#     dbname=conn.schema,
#     user=conn.login,
#     password=conn.password)
#     cursor = connection.cursor()
#     sql = """
#             SELECT *
#             FROM file_sys.file_data
#             WHERE is_expired = 'F';
#         """
#     cursor.execute(sql)
#     print("Connected Database:", conn.schema)
#     records = cursor.fetchall()
#     cursor.close()
#     connection.close()
#     print("====================================")
#     print(f"records type:{type(records)}")
#     print("====================================")
#     print(f"records:\n{records}")
#     print("====================================")
#     print("Fetched Records:")
#     for row in records:
#         print(row)
#     print("====================================")
#     if not records:
#         print("No non-expired files found.")
#     else:
#         print(f"Total files fetched: {len(records)}")
#         for row in records:
#             print(f"File ID: {row[0]}, Path: {row[1]}, Expired: {row[3]}")
#     print("Task completed successfully.")
#     return "done"


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
            
            # TOImplement file deletion logic here
            # For now, just update the deletion_attempted field
            # update_sql = """
                # UPDATE file_sys.file_data
                # SET deletion_attempted = 'T'
                # WHERE file_id = %s;
            # """
            # 
            # cursor.execute(update_sql, (file_id,))
            print(f"Updated file_id {file_id}: deletion_attempted set to 'T'")
        
        # connection.commit()
        print("All files processed successfully.")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        # connection.rollback()
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