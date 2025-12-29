import os
import time
import psycopg2
from psycopg2.extras import execute_values
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dataclasses import dataclass

@dataclass
class PGQueries() :
        # Exchange informations queries
        forex_infos_insert = "INSERT INTO raw.FOREX_EXCHANGE (symbol, forex_name, region, exchange_timezone) VALUES %s ON CONFLICT (symbol) DO NOTHING;"
        index_infos_insert = "INSERT INTO raw.INDEX_EXCHANGE (symbol, currency, index_name, region, exchange_timezone) VALUES%s ON CONFLICT (symbol) DO NOTHING;"
        future_infos_insert = "INSERT INTO raw.FUTURES_EXCHANGE (symbol, currency, futures_name, region, exchange_timezone) VALUES %s ON CONFLICT (symbol) DO NOTHING;"
        crypto_infos_insert = "INSERT INTO raw.CRYPTO_EXCHANGE (symbol, crypto_name, region, exchange_timezone) VALUES %s ON CONFLICT (symbol) DO NOTHING;"
        
        # Values queries
        forex_values_insert = "INSERT INTO raw.FOREX_HISTORY (symbol, value_timestamp, value_open, value_high, value_low, value_close, volume, dividends) VALUES %s ON CONFLICT (symbol, value_timestamp) DO NOTHING"
        index_values_insert = "INSERT INTO raw.INDEX_HISTORY (symbol, value_timestamp, value_open, value_high, value_low, value_close, volume, dividends) VALUES %s ON CONFLICT (symbol, value_timestamp) DO NOTHING"
        future_values_insert = "INSERT INTO raw.FUTURES_HISTORY (symbol, value_timestamp, value_open, value_high, value_low, value_close, volume, dividends) VALUES %s ON CONFLICT (symbol, value_timestamp) DO NOTHING"
        crypto_values_insert = "INSERT INTO raw.CRYPTO_HISTORY (symbol, value_timestamp, value_open, value_high, value_low, value_close, volume, dividends) VALUES %s ON CONFLICT (symbol, value_timestamp) DO NOTHING"
        
        # For the file (ucdp) we will very probably only run a PGSQL query (with the operator) to load the file.
        # The file will be transformed before with pandas to fit the table

def create_sql_operator(task_id : str, sql_file_name : str, dag_path : str = None) -> SQLExecuteQueryOperator :
    print(f"SQL PATH : {os.getenv('SQL_PRODUCTION_FOLDER')}")
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id="postgres",
        sql=os.path.join(dag_path, sql_file_name) if dag_path != None  else sql_file_name
    )

class PGDriver() :

    def __init__(self, user, password, host, dbname):
        self.user = user
        self.password = password
        self.host = host
        self.dbname = dbname

    def connect(self,):
        return psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            dbname=self.dbname
        )
    
    # Inserts a pandas dataframe in batches to postrgresql
    def insert_dataframe(self, df, query, batch_size=100) :
        with self.connect() as conn:
            with conn.cursor() as cursor:
                try:
                    total_rows = len(df)
                    for i in range(0, total_rows, batch_size) :
                        df_chunked = df.iloc[i : i+batch_size]
                        
                        data_tuples = [tuple(x) for x in df_chunked.to_numpy()]
                        execute_values(cursor, query, data_tuples, page_size=batch_size)

                        conn.commit()
                        print(f"Processed rows {i} to {min(i+batch_size, total_rows)}")
                    print("Success: All data inserted.")
                except Exception as e:
                    conn.rollback()
                    print(f"Error: {e}")
                    raise e
 
