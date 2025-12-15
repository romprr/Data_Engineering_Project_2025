from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import yfinance as yf
import pandas as pd
from pymongo import MongoClient
import os
import logging
import glob
from pathlib import Path
import shutil
from utils.file import write as file_write
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

    INGESTION_TEMP_PATH=os.getenv("INGESTION_TEMP_PATH")
    INGESTION_FUTURES_INFORMATION_TEMP_FOLDER=os.getenv("INGESTION_FUTURES_INFORMATION_TEMP_FOLDER")
    INGESTION_FUTURES_VALUES_TEMP_FOLDER=os.getenv("INGESTION_FUTURES_VALUES_TEMP_FOLDER")
    INGESTION_INDICES_INFORMATION_TEMP_FOLDER=os.getenv("INGESTION_INDICES_INFORMATION_TEMP_FOLDER")
    INGESTION_INDICES_VALUES_TEMP_FOLDER=os.getenv("INGESTION_INDICES_VALUES_TEMP_FOLDER")
    INGESTION_FOREX_INFORMATION_TEMP_FOLDER=os.getenv("INGESTION_FOREX_INFORMATION_TEMP_FOLDER")
    INGESTION_FOREX_VALUES_TEMP_FOLDER=os.getenv("INGESTION_FOREX_VALUES_TEMP_FOLDER")
    INGESTION_CRYPTOCURRENCIES_INFORMATION_TEMP_FOLDER=os.getenv("INGESTION_CRYPTOCURRENCIES_INFORMATION_TEMP_FOLDER")
    INGESTION_CRYPTOCURRENCIES_VALUES_TEMP_FOLDER=os.getenv("INGESTION_CRYPTOCURRENCIES_VALUES_TEMP_FOLDER")
    INGESTION_WORLWIDE_EVENTS_TEMP_FOLDER=os.getenv("INGESTION_WORLWIDE_EVENTS_TEMP_FOLDER")

    folders = [
        INGESTION_FUTURES_INFORMATION_TEMP_FOLDER,
        INGESTION_FUTURES_VALUES_TEMP_FOLDER,
        INGESTION_INDICES_INFORMATION_TEMP_FOLDER,
        INGESTION_INDICES_VALUES_TEMP_FOLDER,
        INGESTION_FOREX_INFORMATION_TEMP_FOLDER,
        INGESTION_FOREX_VALUES_TEMP_FOLDER,
        INGESTION_CRYPTOCURRENCIES_INFORMATION_TEMP_FOLDER,
        INGESTION_CRYPTOCURRENCIES_VALUES_TEMP_FOLDER,
        INGESTION_WORLWIDE_EVENTS_TEMP_FOLDER
    ]
    @task 
    def init_env(folders):
        """Initialize environment"""
        print("Initializing environment...")
        for folder in folders:
            if folder is None:
                continue  # Skip if environment variable not set
            path = Path(folder)
            if path.exists():
                shutil.rmtree(path)  # Delete folder and contents
            path.mkdir(parents=True, exist_ok=True)  # Recreate folder
            print(f"Reset folder: {path}")
        print("Temp folder cleared.")
        print("Environment initialized.")

    @task
    def chunk(symbols, chunk_size):
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
    def get_data_info(symbols, folder_path):
        context = get_current_context()
        chunk_index = context["ti"].map_index
        file_path = folder_path + "/"+ chunk_index.__str__() + ".jsonl"
        for symbol in symbols : 
            ticker = yf.Ticker(symbol)
            info = ticker.info
            info["_id"] = symbol
            file_write(info, file_path)
        return file_path

    @task
    def get_data_values(symbols, folder_path):
        context = get_current_context()
        chunk_index = context["ti"].map_index
        file_path = folder_path + "/" + chunk_index.__str__() + ".jsonl"
        for symbol in symbols : 
            ret = {}
            ret["_id"] = symbol
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=f"{20*12}mo", interval="1mo") 
            hist = hist.reset_index()
            data = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
            ret["values"] = data
            file_write(ret, file_path)
        return file_path

    @task
    def load_to_db(file, collection_name):
        """Load chunked file information into the MongoDB"""
        print("loading information into db")
        mongo_client = MongoClient(MONGO_DB_URI)  # Make sure MONGO_DB_URI is set
        db = mongo_client["raw_data_db"]
        collection = db[collection_name]
        with open(file, "r") as f:
            for line in f:
                if not line.strip():
                    continue  # skip empty lines
                doc = json.loads(line)
                # Insert or update document as-is
                if "_id" in doc:
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": doc},
                        upsert=True
                    )
                else:
                    collection.insert_one(doc)
        print("Information loaded into db successfully.")


    init = init_env(folders)

    # ==========================
    # SYMBOLS EXTRACTION TASKS
    # ==========================
    crypto_symbols = get_crypto_symbols()
    forex_symbols = get_forex_symbols()
    futures_symbols = get_futures_symbols()
    indices_symbols = get_indices_symbols()


    # ==========================
    # INFO EXTRACTION TASKS
    # ==========================
    crypto_info_files = get_data_info.partial(folder_path=INGESTION_CRYPTOCURRENCIES_INFORMATION_TEMP_FOLDER).expand(symbols=chunk(crypto_symbols, 10))
    forex_info_files = get_data_info.partial(folder_path=INGESTION_FOREX_INFORMATION_TEMP_FOLDER).expand(symbols=chunk(forex_symbols, 10))
    futures_info_files = get_data_info.partial(folder_path=INGESTION_FUTURES_INFORMATION_TEMP_FOLDER).expand(symbols=chunk(futures_symbols, 10))
    indices_info_files = get_data_info.partial(folder_path=INGESTION_INDICES_INFORMATION_TEMP_FOLDER).expand(symbols=chunk(indices_symbols, 10))

    # ==========================
    # VALUES EXTRACTION TASKS
    # ==========================
    crypto_values_files = get_data_values.partial(folder_path=INGESTION_CRYPTOCURRENCIES_VALUES_TEMP_FOLDER).expand(symbols=chunk(crypto_symbols, 5))
    forex_values_files = get_data_values.partial(folder_path=INGESTION_FOREX_VALUES_TEMP_FOLDER).expand(symbols=chunk(forex_symbols, 5))
    futures_values_files = get_data_values.partial(folder_path=INGESTION_FUTURES_VALUES_TEMP_FOLDER).expand(symbols=chunk(futures_symbols, 5))
    indices_values_files = get_data_values.partial(folder_path=INGESTION_INDICES_VALUES_TEMP_FOLDER).expand(symbols=chunk(indices_symbols, 5))

    # ==========================
    # INFO LOADING TASKS
    # ==========================
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_CRYPTOCURRENCIES_INFORMATION).expand(file=crypto_info_files)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_FOREX_INFORMATION).expand(file=forex_info_files)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_FUTURES_INFORMATION).expand(file=futures_info_files)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_INDICES_INFORMATION).expand(file=indices_info_files)
    
    # ==========================
    # VALUES LOADING TASKS
    # ==========================
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_CRYPTOCURRENCIES_VALUES).expand(file=crypto_values_files)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_FOREX_VALUES).expand(file=forex_values_files)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_FUTURES_VALUES).expand(file=futures_values_files)
    load_to_db.partial(collection_name=MONGO_DB_RAW_DATA_COLLECTION_INDICES_VALUES).expand(file=indices_values_files)

    # ==========================
    # TASK DEPENDENCIES
    # ==========================
    init >> [crypto_symbols, forex_symbols, futures_symbols, indices_symbols]


    


ingestion_pipeline()
