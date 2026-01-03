import os
import time
import psycopg2
from psycopg2.extras import execute_values


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
 
