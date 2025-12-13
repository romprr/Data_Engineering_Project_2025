from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from utils.file import write as write_to_file
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
INGESTION_FUTURES_INFORMATION_TEMP_FILE=os.getenv("INGESTION_FUTURES_INFORMATION_TEMP_FILE")
INGESTION_FUTURES_VALUES_TEMP_FILE=os.getenv("INGESTION_FUTURES_VALUES_TEMP_FILE")
INGESTION_INDICES_INFORMATION_TEMP_FILE=os.getenv("INGESTION_INDICES_INFORMATION_TEMP_FILE")
INGESTION_INDICES_VALUES_TEMP_FILE=os.getenv("INGESTION_INDICES_VALUES_TEMP_FILE")
INGESTION_CURRENCIES_INFORMATION_TEMP_FILE=os.getenv("INGESTION_CURRENCIES_INFORMATION_TEMP_FILE")
INGESTION_CURRENCIES_VALUES_TEMP_FILE=os.getenv("INGESTION_CURRENCIES_VALUES_TEMP_FILE")
INGESTION_CRYPTOCURRENCIES_INFORMATION_TEMP_FILE=os.getenv("INGESTION_CRYPTOCURRENCIES_INFORMATION_TEMP_FILE")
INGESTION_CRYPTOCURRENCIES_PRICES_TEMP_FILE=os.getenv("INGESTION_CRYPTOCURRENCIES_PRICES_TEMP_FILE")
INGESTION_WORLWIDE_EVENTS_TEMP_FILE=os.getenv("INGESTION_WORLWIDE_EVENTS_TEMP_FILE")
WORLD_INDICES_SYMBOLS_SCRAPER_URL=os.getenv("WORLD_INDICES_SYMBOLS_SCRAPER_URL")
FUTURES_SYMBOLS_SCRAPER_URL=os.getenv("FUTURES_SYMBOLS_SCRAPER_URL")
CURRENCIES_SYMBOLS_SCRAPER_URL=os.getenv("CURRENCIES_SYMBOLS_SCRAPER_URL")

WORLDWIDE_EVENTS_CSV_FILE_URL=os.getenv("WORLDWIDE_EVENTS_CSV_FILE_URL")
DOWNLOADS_PATH=os.getenv("DOWNLOADS_PATH")

# ==========================
# PYTHON FUNCTIONS
# ==========================
# INDICES
def extract_indices_symbols():
    """Get indices symbols from Wikipedia"""
    print("Getting the indices symbols")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    df = pd.read_html(WORLD_INDICES_SYMBOLS_SCRAPER_URL, storage_options=headers)[0] # First table on the page
    symbols = df['Symbol'].dropna().tolist() # the nan values make the xcom fail
    print("Finished getting the indices symbols")
    print("Indices symbols:", symbols)
    return symbols

def extract_indices_values(file_path, **context): 
    """Get indices values using yfinance"""
    print("Extracting indices values in file:", file_path)
    symbols = context['ti'].xcom_pull(task_ids='get_indices_symbols')
    for symbol in symbols:
        indices_values = {}
        print("GETTING VALUE FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{12 * 20}mo", interval="1wk")
        if not hist.empty:
            hist = hist.reset_index()
            data = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
            indices_values[symbol] = data
        else:
            indices_values[symbol] = None
        write_to_file(indices_values, file_path) # writing after each symbol to avoid data loss
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
    print("Finished extracting indices values in file:", file_path)

def extract_indices_info(file_path, **context): 
    """Get indices information using yfinance"""
    print("Extracting indices information in file:", file_path)
    symbols = context['ti'].xcom_pull(task_ids='get_indices_symbols')
    for symbol in symbols:
        indices_info = {}
        print("GETTING INFO FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        info = ticker.info
        indices_info[symbol] = info
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
        write_to_file(indices_info, file_path)
    print("Finished extracting indices information in file:", file_path)

def extract_forex_symbols():
    """Get forex symbols from Wikipedia"""
    print("Getting the forex symbols")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    df = pd.read_html(CURRENCIES_SYMBOLS_SCRAPER_URL, storage_options=headers)[0] # First table on the page
    symbols = df['Symbol'].dropna().tolist() # the nan values make the xcom fail
    print("Finished getting the forex symbols")
    print("Forex symbols:", symbols)
    return symbols

def extract_forex_values(file_path, **context):
    """Get forex values using yfinance"""
    print("Extracting forex values in file:", file_path)
    symbols = context['ti'].xcom_pull(task_ids='get_forex_symbols')
    for symbol in symbols:
        forex_values = {}
        print("GETTING VALUE FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{12 * 20}mo", interval="1wk")
        if not hist.empty:
            hist = hist.reset_index()
            data = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
            forex_values[symbol] = data
        else:
            forex_values[symbol] = None
        write_to_file(forex_values, file_path) # writing after each symbol to avoid data loss
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
    print("Finished extracting forex values in file:", file_path)

def extract_forex_info(file_path, **context):
    """Get forex information using yfinance"""
    print("Extracting forex information in file:", file_path)
    symbols = context['ti'].xcom_pull(task_ids='get_forex_symbols')
    for symbol in symbols:
        forex_info = {}
        print("GETTING INFO FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        info = ticker.info
        forex_info[symbol] = info
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
        write_to_file(forex_info, file_path)
    print("Finished extracting forex information in file:", file_path)

def extract_crypto_symbols(): 
    """Get cryptocurrency symbols"""
    return ["BTC-USD", "ETH-USD"]

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

def extract_futures_symbols():
    """Get futures symbols from Wikipedia"""
    print("Getting the futures symbols")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    df = pd.read_html(FUTURES_SYMBOLS_SCRAPER_URL, storage_options=headers)[0] # First table on the page
    symbols = df['Symbol'].dropna().tolist() # the nan values make the xcom fail
    print("Finished getting the futures symbols")
    print("Futures symbols:", symbols)
    return symbols

def extract_futures_values(file_path, **context):
    """Get futures values using yfinance"""
    print("Extracting futures values in file:", file_path)
    symbols = context['ti'].xcom_pull(task_ids='get_futures_symbols')
    for symbol in symbols:
        futures_values = {}
        print("GETTING VALUE FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=f"{12 * 20}mo", interval="1wk")
        if not hist.empty:
            hist = hist.reset_index()
            data = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
            futures_values[symbol] = data
        else:
            futures_values[symbol] = None
        write_to_file(futures_values, file_path) # writing after each symbol to avoid data loss
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
    print("Finished extracting futures values in file:", file_path)

def extract_futures_info(file_path, **context):
    """Get futures information using yfinance"""
    print("Extracting futures information in file:", file_path)
    symbols = context['ti'].xcom_pull(task_ids='get_futures_symbols')
    for symbol in symbols:
        futures_info = {}
        print("GETTING INFO FOR SYMBOL:", symbol)
        ticker = yf.Ticker(symbol)
        info = ticker.info
        futures_info[symbol] = info
        time.sleep(2.0)  # staggered timing to avoid parallel request collision
        write_to_file(futures_info, file_path)
    print("Finished extracting futures information in file:", file_path)



default_args = {
    "owner": "niceJobTeam",
    "depends_on_past": False, # do not depend on past runs
    "retries": 3, # number of retries on failure
    "retry_delay": timedelta(minutes=5), # wait time between retries
    "email_on_failure": False, # disable email on failure
}

@dag(
        dag_id="ingestion_pipeline",
        default_args=default_args, 
        schedule="@daily", 
        start_date=datetime.now() - timedelta(days=1), 
        description="The pipeline that will get the data from different sources and insert it into the Mongo database",
        catchup=False, 
        tags=["ingestion"]
    )
def ingestion_pipeline():
    """Ingestion DAG to extract data from various sources and load into MongoDB"""

    @task 
    def init_env():
        """Initialize environment"""
        print("Initializing environment...")
        print("Environment initialized.")

    @task
    def chunk_symbols(symbols, chunk_size=5):
        return [
            symbols[i : i + chunk_size]
            for i in range(0, len(symbols), chunk_size)
        ]

    @task
    def get_crypto_symbols():
        print("Getting crypto symbols...")
        return ["BTC-USD", "ETH-USD"]

    @task
    def get_forex_symbols():
        """Get forex symbols from Wikipedia"""
        print("Getting the forex symbols")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        df = pd.read_html(CURRENCIES_SYMBOLS_SCRAPER_URL, storage_options=headers)[0] # First table on the page
        symbols = df['Symbol'].dropna().tolist() # the nan values make the xcom fail
        print("Finished getting the forex symbols")
        print("Forex symbols:", symbols)
        return symbols
    
    @task
    def get_futures_symbols():
        """Get futures symbols from Wikipedia"""
        print("Getting the futures symbols")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        df = pd.read_html(FUTURES_SYMBOLS_SCRAPER_URL, storage_options=headers)[0] # First table on the page
        symbols = df['Symbol'].dropna().tolist() # the nan values make the xcom fail
        print("Finished getting the futures symbols")
        print("Futures symbols:", symbols)
        return symbols
    
    @task
    def get_indices_symbols():
        """Get indices symbols from Wikipedia"""
        print("Getting the indices symbols")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        df = pd.read_html(WORLD_INDICES_SYMBOLS_SCRAPER_URL, storage_options=headers)[0] # First table on the page
        symbols = df['Symbol'].dropna().tolist() # the nan values make the xcom fail
        print("Finished getting the indices symbols")
        print("Indices symbols:", symbols)
        return symbols

    @task
    def get_data_info(symbols):
        ret = {}
        for symbol in symbols : 
            ticker = yf.Ticker(symbol)
            info = ticker.info
            ret[symbol] = info
        return ret   

    @task
    def get_data_values(symbols):
        ret = {}
        for symbol in symbols : 
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=f"{12 * 20}mo", interval="1wk") 
            if not hist.empty:
                hist = hist.reset_index()
                data = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
                ret[symbol] = data
            else:
                ret[symbol] = None
        return ret

    @task 
    def load_to_db(data, collection):
        """Load data into the mongo db"""
        
   


    init = init_env()
    crypto_symbols = get_crypto_symbols()
    forex_symbols = get_forex_symbols()
    futures_symbols = get_futures_symbols()
    indices_symbols = get_indices_symbols()
    init >> [crypto_symbols, forex_symbols, futures_symbols, indices_symbols]

    crypto_symbol_chunks = chunk_symbols(crypto_symbols)
    forex_symbols_chunks = chunk_symbols(forex_symbols)
    futures_symbols_chunks = chunk_symbols(futures_symbols)
    indices_symbols_chunks = chunk_symbols(indices_symbols)

    crypto_info = get_data_info.expand(symbols=crypto_symbol_chunks)
    forex_info = get_data_info.expand(symbols=forex_symbols_chunks)
    futures_info = get_data_info.expand(symbols=futures_symbols_chunks)
    indices_info = get_data_info.expand(symbols=indices_symbols_chunks)

    crypto_values = get_data_values.expand(symbols=crypto_symbol_chunks)
    forex_values = get_data_values.expand(symbols=forex_symbols_chunks)
    futures_values = get_data_values.expand(symbols=futures_symbols_chunks)
    indices_values = get_data_values.expand(symbols=indices_symbols_chunks)
    

    


ingestion_pipeline()
