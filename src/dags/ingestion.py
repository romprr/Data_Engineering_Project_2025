from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from utils.csv import read as read_csv
from utils.api import query as query_api
import os
import logging

# Getting environment variables
COMPANIES_SYMBOLS_FILEPATH = os.getenv("COMPANIES_SYMBOLS_FILEPATH")
ALPHA_VANTAGE_BASE_URL = os.getenv("ALPHA_VANTAGE_BASE_URL")
ALPHA_VANTAGE_API_TOKEN = os.getenv("ALPHA_VANTAGE_API_TOKEN")
ALPHA_VANTAGE_STOCK_DATA_FUNCTION = os.getenv("ALPHA_VANTAGE_STOCK_DATA_FUNCTION")
ALPHA_VANTAGE_COMPANY_INFO_FUNCTION = os.getenv("ALPHA_VANTAGE_COMPANY_INFO_FUNCTION")

# Local functions
def extract_companies_symbols(file_path):
    companies = read_csv(file_path)
    return [company['SYMBOL'] for company in companies]  # Changed to uppercase

def fetch_alpha_vantage_data(symbols, function_name, additional_query_params={}):
    data = {}
    for symbol in symbols:
        params = { # Basic required params
            "function": function_name, 
            "symbol": symbol,
            "apikey": ALPHA_VANTAGE_API_TOKEN
        }
        params.update(additional_query_params) # adding any additional params
        response = query_api(ALPHA_VANTAGE_BASE_URL, params=params)
        data[symbol] = response
    return data

default_args = {
    "owner": "niceJobTeam",
    "depends_on_past": False, # do not depend on past runs
    "retries": 3, # number of retries on failure
    "retry_delay": timedelta(minutes=5), # wait time between retries
    "email_on_failure": False, # disable email on failure
}

with DAG(
    dag_id="ingestion_pipeline",
    default_args=default_args,
    description="The pipeline that will get the data from different sources and insert it into the Mongo database",
    schedule="@daily",
    start_date=datetime.now() - timedelta(days=1), # yesterday
    catchup=False,
    tags=["ingestion"],
) as dag:

    # Empty operators
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Exrtraction tasks
    get_companies_symbols = PythonOperator(
        task_id="get_companies_symbols",
        python_callable=extract_companies_symbols,
        op_kwargs={"file_path": COMPANIES_SYMBOLS_FILEPATH},
        do_xcom_push=True, # push the result to XCom
        retry_delay=timedelta(minutes=2), # override default retry delay, it is a file read operation so it should be quicker that querieng an API
    )

    # Cannot use SimpleHttpOperator beause the list of symbols got extracted from the CSV at runtime
    get_stock_prices = PythonOperator(
        task_id="get_stock_prices",
        python_callable=fetch_alpha_vantage_data,  # Generic function for fetching Alpha Vantage data
        op_kwargs={
            "symbols": "{{ ti.xcom_pull(task_ids='get_companies_symbols') }}",
            "function_name": ALPHA_VANTAGE_STOCK_DATA_FUNCTION, 
            "additional_query_params": {
                "interval": "60min",  # Interval between the stock prices
            }
        },
        do_xcom_push=True
    )

    get_companies_general_info = PythonOperator(
        task_id="get_companies_general_info",
        python_callable=fetch_alpha_vantage_data,  # Same generic function, different function_name
        op_kwargs={
            "symbols": "{{ ti.xcom_pull(task_ids='get_companies_symbols') }}",
            "function_name": ALPHA_VANTAGE_COMPANY_INFO_FUNCTION
        },
        do_xcom_push=True
    )

    # Task dependencies
    start >> get_companies_symbols >> [get_stock_prices, get_companies_general_info] >> end




    


