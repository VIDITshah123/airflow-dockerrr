FROM apache/airflow:3.0.4

USER airflow

RUN pip install --no-cache-dir \
    pycryptodomex \
    fpdf \
    pandas \
    numpy \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-redis \
    apache-airflow-providers-mongo \
    apache-airflow-providers-postgres \
    openpyxl \
    pyodbc \
    pymongo \
    psycopg2 
    
    

