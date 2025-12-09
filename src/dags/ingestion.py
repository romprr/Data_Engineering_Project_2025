from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils.file import write as write_to_file
from utils.api import query as query_api
import yfinance as yf
import pandas as pd
import time
import os
from utils.file import write as write_to_file
import logging
import json

yf.set_tz_cache_location("/tmp/yfinance_cache")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# Constants 
WIKIPEDIA_SP500_URL = os.getenv("WIKIPEDIA_SP500_URL")
COINMARKETCAP_LIST_CRYPTO_SYMBOLS_URL = os.getenv("COINMARKETCAP_LIST_CRYPTO_SYMBOLS_URL")
INGESTION_STOCKS_TRANSACTIONS_TEMP_FILE=os.getenv("INGESTION_STOCKS_TRANSACTIONS_TEMP_FILE")
INGESTION_COMPANIES_TEMP_FILE=os.getenv("INGESTION_COMPANIES_TEMP_FILE")
INGESTION_STOCK_PRICES_TEMP_FILE=os.getenv("INGESTION_STOCK_PRICES_TEMP_FILE")
INGESTION_CRYPTO_PRICES_TEMP_FILE=os.getenv("INGESTION_CRYPTO_PRICES_TEMP_FILE")
INGESTION_CRYPTO_INFO_TEMP_FILE=os.getenv("INGESTION_CRYPTO_INFO_TEMP_FILE")
INGESTION_EVENTS_TEMP_FILE=os.getenv("INGESTION_EVENTS_TEMP_FILE")
INGESTION_POLITICIANS_TEMP_FILE=os.getenv("INGESTION_POLITICIANS_TEMP_FILE")
COIN_API_TOKEN=os.getenv("COIN_API_TOKEN")
COIN_API_BASE_URL=os.getenv("COIN_API_BASE_URL")
UCDP_POLITICAL_EVENTS_CSV_FILE_URL=os.getenv("UCDP_POLITICAL_EVENTS_CSV_FILE_URL")
DOWNLOADS_PATH=os.getenv("DOWNLOADS_PATH")

# ==========================
# PYTHON FUNCTIONS
# ==========================
# STOCKS
def extract_companies_symbols():
    """Get company symbols from Wikipedia"""
    print("Getting the companies symbols")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    df = pd.read_html(WIKIPEDIA_SP500_URL, storage_options=headers)[0] # First table on the page
    symbols = df['Symbol'].tolist()
    print("Finished getting the companies symbols")
    print(symbols)
    return symbols

def extract_stock_prices(file_path, **context): 
    """Get stock prices for given company symbols using yfinance"""
    print("Extracting stock prices in file:", file_path)
    symbols = context['ti'].xcom_pull(task_ids='get_companies_symbols')
    for symbol in symbols:
        stock_prices = {}
        print("GETTING PRICE FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{12 * 20}mo", interval="1wk")
        if not hist.empty:
            hist = hist.reset_index()
            data = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
            stock_prices[symbol] = data
        else:
            stock_prices[symbol] = None
        write_to_file(stock_prices, file_path) # writing after each symbol to avoid data loss
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
    print("Finished extracting stock prices in file:", file_path)

def extract_companies_info(file_path, **context):
    """Get company information for given symbols using yfinance"""
    print("getting the companies information")
    symbols = context['ti'].xcom_pull(task_ids='get_companies_symbols')
    for symbol in symbols:
        companies_info = {}
        print("GETTING INFO FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        info = ticker.info
        companies_info[symbol] = info
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
        write_to_file(companies_info, file_path) 
    print("finished getting the companies information")

# CRYPTOCURRENCIES
def extract_crypto_symbols(): 
    """Get cryptocurrency symbols"""
    print("Getting the cryptocurrency symbols")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    df = pd.read_html("https://bitcoinwiki.org/wiki/cryptocurrency-list", storage_options=headers)[0] # First table on the page
    symbols = [symbols + "-USD" for symbols in df['Symbol'].tolist()]
    print("Finished getting the cryptocurrency symbols")
    return symbols

def extract_crypto_prices(file_path, **context): 
    """Get cryptocurrency prices using binance API"""
    crypto_symbols = context['ti'].xcom_pull(task_ids='get_crypto_symbols')
    for symbol in crypto_symbols:
        crypto_data = {}
        print("GETTING PRICE FOR SYMBOL:", symbol)      
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{12 * 20}mo", interval="1wk") 
        if not hist.empty:
            hist = hist.reset_index()
            data = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
            crypto_data[symbol] = data
        else:
            crypto_data[symbol] = None
        write_to_file(crypto_data, file_path)
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
    print("Finished extracting cryptocurrency prices in file:", file_path)

def extract_crypto_info(file_path, **context): 
    """Get cryptocurrency information using yfinance"""
    crypto_symbols = context['ti'].xcom_pull(task_ids='get_crypto_symbols')
    for symbol in crypto_symbols:
        crypto_info = {}
        print("GETTING INFO FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        info = ticker.info
        crypto_info[symbol] = info
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
        write_to_file(crypto_info, file_path)
    print("Finished extracting cryptocurrency information in file:", file_path)

# POLITICS
def extract_political_events(file_path):
    """Get political events from UCDP API"""
    print("Extracting political events in file:", file_path)
    political_events = query_api("https://ucdpapi.example.com/political_events")  # Placeholder URL
    for event in political_events:
        write_to_file(event, file_path)
    print("Finished extracting political events in file:", file_path)

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

    init_env = BashOperator(
        task_id="init_env", 
        bash_command=f'''
        truncate -s 0 {INGESTION_STOCKS_TRANSACTIONS_TEMP_FILE} && \
        truncate -s 0 {INGESTION_COMPANIES_TEMP_FILE} && \
        truncate -s 0 {INGESTION_STOCK_PRICES_TEMP_FILE} && \
        truncate -s 0 {INGESTION_CRYPTO_PRICES_TEMP_FILE} && \
        truncate -s 0 {INGESTION_CRYPTO_INFO_TEMP_FILE} && \
        truncate -s 0 {INGESTION_EVENTS_TEMP_FILE} && \
        truncate -s 0 {INGESTION_POLITICIANS_TEMP_FILE} 
        '''
    )
    end = EmptyOperator(task_id="end")

    # ==========================
    # Extraction tasks
    # ==========================
    # STOCKS
    get_companies_symbols = PythonOperator(
        task_id="get_companies_symbols",
        python_callable=extract_companies_symbols,
        do_xcom_push=True # push the result to XCom
    )

    get_stock_prices = PythonOperator( 
        task_id="get_stock_prices",
        python_callable=extract_stock_prices,
        op_kwargs={"file_path": INGESTION_STOCK_PRICES_TEMP_FILE},
        do_xcom_push=False
    )

    get_companies_info = PythonOperator( 
        task_id="get_companies_info",
        python_callable=extract_companies_info,
        op_kwargs={"file_path": INGESTION_COMPANIES_TEMP_FILE},
        do_xcom_push=False
    )

    # CRYPTOCURRENCIES
    get_crypto_symbols = PythonOperator(
        task_id="get_crypto_symbols",
        python_callable=extract_crypto_symbols,
        do_xcom_push=True
    )

    # get_crypto_prices = PythonOperator(
    #     task_id="get_crypto_prices",
    #     python_callable=extract_crypto_prices,
    #     op_kwargs={"file_path": INGESTION_CRYPTO_PRICES_TEMP_FILE},
    #     do_xcom_push=False
    # )
    
    # get_crypto_info = PythonOperator(
    #     task_id="get_crypto_info",
    #     python_callable=extract_crypto_info,
    #     op_kwargs={"file_path": INGESTION_CRYPTO_INFO_TEMP_FILE},
    #     do_xcom_push=False
    # )

    # POLITICS
    get_political_events_file = BashOperator(
        task_id="get_political_events_file",
        bash_command=f'''
        curl -o {DOWNLOADS_PATH}/political_events.zip {UCDP_POLITICAL_EVENTS_CSV_FILE_URL} && \ 
        unzip -o {DOWNLOADS_PATH}/political_events.zip -d {DOWNLOADS_PATH} && \ 
        rm {DOWNLOADS_PATH}/political_events.zip
        '''
    )

    get_politicians_info # TODO
    
    # ==========================
    # TASK DEPENDENCIES
    # =========================
    # init_env >> [get_companies_symbols, get_crypto_symbols, get_political_events_file, get_politicians_info] 
    # get_companies_symbols >> [get_stock_prices, get_companies_info, get_stock_transactions]
    # get_crypto_symbols >> [get_crypto_prices, get_crypto_info, get_crypto_transactions]
    # get_stock_prices >> insert_stock_prices_to_db
    # get_companies_info >> insert_companies_info_to_db
    # get_stock_transactions >> insert_stock_transactions_to_db
    # get_crypto_prices >> insert_crypto_prices_to_db
    # get_crypto_info >> insert_crypto_info_to_db
    # get_crypto_transactions >> insert_crypto_transactions_to_db
    # get_political_events_file >> insert_political_events_to_db
    # get_politicians_info >> insert_politicians_info_to_db
    # [insert_stock_prices_to_db, insert_companies_info_to_db, insert_stock_transactions_to_db,
    #  insert_crypto_prices_to_db, insert_crypto_info_to_db, insert_crypto_transactions_to_db,
    #  insert_political_events_to_db, insert_politicians_info_to_db] >> end

    init_env >> [get_companies_symbols, get_crypto_symbols] 
    get_companies_symbols >> [get_stock_prices, get_companies_info]
    # get_crypto_symbols >> [get_crypto_prices, get_crypto_info]

    get_political_events_file


    

