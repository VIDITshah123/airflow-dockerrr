from airflow.hooks.base import BaseHook
import psycopg2

class DBConnector:
    
    def select_statement(self, schema_name, table_name, column_name, condition):

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
            sql = f""" 
                SELECT {column_name} from {schema_name}.{table_name}
                WHERE {condition};
            """
            cursor.execute(sql)
            records = cursor.fetchall()
            return records

        except Exception as e:
            print(f"Error in select_statement: {e}")
            return None
        finally:
            cursor.close()
            connection.close()
        
