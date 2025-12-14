from datetime import datetime, timedelta

from airflow.decorators import dag, task
import yfinance as yf
import pandas as pd
from pymongo import MongoClient
import os
import logging
import json

yf.set_tz_cache_location("/tmp/yfinance_cache")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# Constants 
INGESTION_FUTURES_INFORMATION_TEMP_FILE=os.getenv("INGESTION_FUTURES_INFORMATION_TEMP_FILE")
INGESTION_FUTURES_VALUES_TEMP_FILE=os.getenv("INGESTION_FUTURES_VALUES_TEMP_FILE")
INGESTION_INDICES_INFORMATION_TEMP_FILE=os.getenv("INGESTION_INDICES_INFORMATION_TEMP_FILE")
INGESTION_INDICES_VALUES_TEMP_FILE=os.getenv("INGESTION_INDICES_VALUES_TEMP_FILE")
INGESTION_FOREX_INFORMATION_TEMP_FILE=os.getenv("INGESTION_FOREX_INFORMATION_TEMP_FILE")
INGESTION_FOREX_VALUES_TEMP_FILE=os.getenv("INGESTION_FOREX_VALUES_TEMP_FILE")
INGESTION_CRYPTOFOREX_INFORMATION_TEMP_FILE=os.getenv("INGESTION_CRYPTOFOREX_INFORMATION_TEMP_FILE")
INGESTION_CRYPTOFOREX_PRICES_TEMP_FILE=os.getenv("INGESTION_CRYPTOFOREX_PRICES_TEMP_FILE")
INGESTION_WORLWIDE_EVENTS_TEMP_FILE=os.getenv("INGESTION_WORLWIDE_EVENTS_TEMP_FILE")
WORLD_INDICES_SYMBOLS_SCRAPER_URL=os.getenv("WORLD_INDICES_SYMBOLS_SCRAPER_URL")
FUTURES_SYMBOLS_SCRAPER_URL=os.getenv("FUTURES_SYMBOLS_SCRAPER_URL")
FOREX_SYMBOLS_SCRAPER_URL=os.getenv("FOREX_SYMBOLS_SCRAPER_URL")

WORLDWIDE_EVENTS_CSV_FILE_URL=os.getenv("WORLDWIDE_EVENTS_CSV_FILE_URL")
DOWNLOADS_PATH=os.getenv("DOWNLOADS_PATH")

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
    MONGO_DB_RAW_DATA_COLLECTION_FUTURES_INFORMATION=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_FUTURES_INFORMATION")
    MONGO_DB_RAW_DATA_COLLECTION_FUTURES_VALUES=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_FUTURES_VALUES")
    MONGO_DB_RAW_DATA_COLLECTION_INDICES_INFORMATION=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_INDICES_INFORMATION")
    MONGO_DB_RAW_DATA_COLLECTION_INDICES_VALUES=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_INDICES_VALUES")
    MONGO_DB_RAW_DATA_COLLECTION_FOREX_INFORMATION=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_FOREX_INFORMATION")
    MONGO_DB_RAW_DATA_COLLECTION_FOREX_VALUES=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_FOREX_VALUES")
    MONGO_DB_RAW_DATA_COLLECTION_CRYPTOCURRENCIES_INFORMATION=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_CRYPTOCURRENCIES_INFORMATION")
    MONGO_DB_RAW_DATA_COLLECTION_CRYPTOCURRENCIES_VALUES=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_CRYPTOCURRENCIES_VALUES")
    MONGO_DB_RAW_DATA_COLLECTION_WORLWIDE_EVENTS=os.getenv("MONGO_DB_RAW_DATA_COLLECTION_WORLWIDE_EVENTS")
    MONGO_DB_URI=os.getenv("MONGO_DB_URI")
    @task 
    def init_env():
        """Initialize environment"""
        print("Initializing environment...")
        print("Environment initialized.")

    @task
    def chunk(symbols, chunk_size=7):
        return [
            symbols[i : i + chunk_size]
            for i in range(0, len(symbols), chunk_size)
        ]
    
    @task
    def end():
        print("Ingestion pipeline completed successfully.")

    @task
    def get_crypto_symbols():
        print("Getting crypto symbols...")
        return ["BTC-USD", "ETH-USD"]

    @task
    def get_forex_symbols():
        """Get forex symbols from Wikipedia"""
        print("Getting the forex symbols")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        df = pd.read_html(FOREX_SYMBOLS_SCRAPER_URL, storage_options=headers)[0] # First table on the page
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
            hist = ticker.history(period="1mo", interval="1wk") 
            if not hist.empty:
                hist = hist.reset_index()
                data = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
                ret[symbol] = data
            else:
                ret[symbol] = None
        return ret

    @task
    def transform(chunk):
        documents = [] # documents creation that will hold the data 
        for key in chunk.keys() :
            doc = {}
            doc["_id"] = key
            doc["values"] = chunk[key]
            documents.append(doc)
        print(f"finished, the list of documents is : {documents}")
        return documents

    @task
    def load_to_db(chunk, collection_name):
        """Load chunked information into the MongoDB"""
        print("loading information into db")
        mongo_client = MongoClient(MONGO_DB_URI)  # Make sure MONGO_DB_URI is set
        db = mongo_client["raw_data_db"]
        collection = db[collection_name]
        for doc in chunk:
            collection.update_one(
                {"_id": doc["_id"]}, 
                {"$set": {"values": doc["values"]}},
                upsert=True
            )
        print("Information loaded into db successfully.")


    init = init_env()

    # ==========================
    # SYMBOLS EXTRACTION TASKS
    # ==========================
    crypto_symbols = get_crypto_symbols()
    forex_symbols = get_forex_symbols()
    futures_symbols = get_futures_symbols()
    indices_symbols = get_indices_symbols()

    # ==========================
    # DATA CHUNKING
    # ==========================
    crypto_symbol_chunks = chunk(crypto_symbols)
    forex_symbols_chunks = chunk(forex_symbols)
    futures_symbols_chunks = chunk(futures_symbols)
    indices_symbols_chunks = chunk(indices_symbols)

    # ==========================
    # INFO EXTRACTION TASKS
    # ==========================
    crypto_info = get_data_info.expand(symbols=crypto_symbol_chunks)
    forex_info = get_data_info.expand(symbols=forex_symbols_chunks)
    futures_info = get_data_info.expand(symbols=futures_symbols_chunks)
    indices_info = get_data_info.expand(symbols=indices_symbols_chunks)

    # ==========================
    # VALUES EXTRACTION TASKS
    # ==========================
    crypto_values = get_data_values.expand(symbols=crypto_symbol_chunks)
    forex_values = get_data_values.expand(symbols=forex_symbols_chunks)
    futures_values = get_data_values.expand(symbols=futures_symbols_chunks)
    indices_values = get_data_values.expand(symbols=indices_symbols_chunks)


    # ==========================
    # TRANSFORMATION TASKS
    # ==========================
    jsoned_crypto_values = transform.expand(chunk=crypto_values)
    jsoned_crypto_info = transform.expand(chunk=crypto_info)
    jsoned_forex_values = transform.expand(chunk=forex_values)
    jsoned_forex_info = transform.expand(chunk=forex_info)
    jsoned_futures_values = transform.expand(chunk=futures_values)
    jsoned_futures_info = transform.expand(chunk=futures_info)
    jsoned_indices_values = transform.expand(chunk=indices_values)
    jsoned_indices_info = transform.expand(chunk=indices_info)

    # ==========================
    # LOADING TASKS
    # ==========================
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_CRYPTOCURRENCIES_INFORMATION).expand(chunk=jsoned_crypto_info)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_CRYPTOCURRENCIES_VALUES).expand(chunk=jsoned_crypto_values)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_FOREX_INFORMATION).expand(chunk=jsoned_forex_info)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_FOREX_VALUES).expand(chunk=jsoned_forex_values)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_FUTURES_INFORMATION).expand(chunk=jsoned_futures_info)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_FUTURES_VALUES).expand(chunk=jsoned_futures_values)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_INDICES_INFORMATION).expand(chunk=jsoned_indices_info)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_INDICES_VALUES).expand(chunk=jsoned_indices_values)
    
    # ==========================
    # TASK DEPENDENCIES
    # ==========================
    init >> [crypto_symbols, forex_symbols, futures_symbols, indices_symbols]


    


ingestion_pipeline()
