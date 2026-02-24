from airflow.hooks.base import BaseHook
import psycopg2

class DBConnector:
    
    def select_statement(self, selector, update_values, schema_name="file_sys", table_name="file_data", column_name="*", condition="None"):

        conn = BaseHook.get_connection('postgres_airflow')
        
        connection = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password
        )
        try:
            cursor = connection.cursor()

            # ---------- CASE 1: SELECT ----------
            if selector == "SELECT":

                # SELECT with WHERE
                if condition and condition.lower() != "none":
                    sql = f"""
                        SELECT {column_name}
                        FROM {schema_name}.{table_name}
                        WHERE {condition};
                    """

                # SELECT without WHERE
                else:
                    sql = f"""
                        SELECT {column_name}
                        FROM {schema_name}.{table_name};
                    """

                cursor.execute(sql)
                records = cursor.fetchall()

                print(f"[INFO] Rows fetched: {len(records)}")
                return records

            # ---------- CASE 2: UPDATE ----------
            elif selector == "UPDATE":

                if not condition or condition.lower() == "none":
                    raise ValueError("UPDATE requires a WHERE condition.")

                sql = f"""
                    UPDATE {schema_name}.{table_name}
                    SET {update_values}
                    WHERE {condition};
                """

                cursor.execute(sql)
                connection.commit()

                affected = cursor.rowcount
                print(f"[INFO] Rows updated: {affected}")
                return affected

            # ---------- CASE 3: DELETE ----------
            elif selector == "DELETE":

                if not condition or condition.lower() == "none":
                    raise ValueError("DELETE requires a WHERE condition.")

                sql = f"""
                    DELETE FROM {schema_name}.{table_name}
                    WHERE {condition};
                """

                cursor.execute(sql)
                connection.commit()

                affected = cursor.rowcount
                print(f"[INFO] Rows deleted: {affected}")
                return affected

            # ---------- INVALID SELECTOR ----------
            else:
                raise ValueError(
                    "Invalid selector. Use SELECT, UPDATE, or DELETE."
                )

        except Exception as e:
            print(f"[ERROR] DB operation failed: {e}")
            return None

        finally:
            if "cursor" in locals():
                cursor.close()
            if "connection" in locals():
                connection.close()
        
