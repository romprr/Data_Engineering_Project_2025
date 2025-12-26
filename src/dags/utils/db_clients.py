import os
import time
import psycopg2
from psycopg2.extras import execute_values
from dataclasses import dataclass

@dataclass
class PGQueries() :
        # Exchange informations queries
        forex_infos_insert = "INSERT INTO FOREX_EXCHANGE (symbol, forex_name, region, exchange_timezone) VALUES %s ON CONFLICT (symbol) DO NOTHING;"
        index_infos_insert = "INSERT INTO INDEX_EXCHANGE (symbol, currency, index_name, region, exchange_timezone) VALUES%s ON CONFLICT (symbol) DO NOTHING;"
        future_infos_insert = "INSERT INTO FUTURES_EXCHANGE (symbol, currency, futures_name, region, exchange_timezone) VALUES %s ON CONFLICT (symbol) DO NOTHING;"
        crypto_infos_insert = "INSERT INTO CRYPTO_EXCHANGE (symbol, crypto_name, region, exchange_timezone) VALUES %s ON CONFLICT (symbol) DO NOTHING;"
        
        # Values queries
        forex_values_insert = "INSERT INTO FOREX_HISTORY (symbol, value_timestamp, value_open, value_high, value_low, value_close, volume, dividends) VALUES %s ON CONFLICT (symbol, value_timestamp) DO NOTHING"
        index_values_insert = "INSERT INTO INDEX_HISTORY (symbol, value_timestamp, value_open, value_high, value_low, value_close, volume, dividends) VALUES %s ON CONFLICT (symbol, value_timestamp) DO NOTHING"
        future_values_insert = "INSERT INTO FUTURES_HISTORY (symbol, value_timestamp, value_open, value_high, value_low, value_close, volume, dividends) VALUES %s ON CONFLICT (symbol, value_timestamp) DO NOTHING"
        crypto_values_insert = "INSERT INTO CRYPTO_HISTORY (symbol, value_timestamp, value_open, value_high, value_low, value_close, volume, dividends) VALUES %s ON CONFLICT (symbol, value_timestamp) DO NOTHING"
        
        # For the file (ucdp) we will very probably only run a PGSQL query (with the operator) to load the file.
        # The file will be transformed before with pandas to fit the table


class PGDriver() :

    def connect(self):
        return psycopg2.connect(
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host='postgres-db',
            dbname=os.getenv('POSTGRES_DB')
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
 
