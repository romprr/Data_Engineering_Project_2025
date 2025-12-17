from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import yfinance as yf
import pandas as pd
import utils.mongo as mongo
import utils.redis as redis
import os
import logging
import glob
from pathlib import Path
import shutil
import requests
from utils.file import write as file_write
import json
import zipfile

yf.set_tz_cache_location("/tmp/yfinance_cache")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# Constants 
INDICES_SYMBOLS_SCRAPER_URL=os.getenv("INDICES_SYMBOLS_SCRAPER_URL")
FUTURES_SYMBOLS_SCRAPER_URL=os.getenv("FUTURES_SYMBOLS_SCRAPER_URL")
FOREX_SYMBOLS_SCRAPER_URL=os.getenv("FOREX_SYMBOLS_SCRAPER_URL")
WORLDWIDE_EVENTS_CSV_FILE_URL=os.getenv("WORLDWIDE_EVENTS_CSV_FILE_URL")
SHARED_FOLDER_PATH_AIRFLOW=os.getenv("SHARED_FOLDER_PATH_AIRFLOW")

MONGO_DB_RAW_DATA_COLLECTION=os.getenv("MONGO_DB_RAW_DATA_COLLECTION")
MONGO_DB_URI=os.getenv("MONGO_DB_URI")

REDIS_URI = "redis://crud:crud@redis-1:6379/0"

ASSETS_TYPES = ["forex", "futures", "indices", "crypto"]
ASSETS_SCRAPPER_URLS = [
    FOREX_SYMBOLS_SCRAPER_URL,
    FUTURES_SYMBOLS_SCRAPER_URL,
    INDICES_SYMBOLS_SCRAPER_URL,
    None  # Crypto symbols are hardcoded
]


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
        schedule=None, 
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
        print("Environment initialized.")

    @task
    def chunk_list(symbols, chunk_size):
        return [
            symbols[i : i + chunk_size]
            for i in range(0, len(symbols), chunk_size)
        ]
    
    @task
    def end():
        print("Ingestion pipeline completed successfully.")

    @task
    def get_asset_symbols(asset_type, URL):
        if URL:
            print(f"Getting the {asset_type} symbols from {URL}")
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            df = pd.read_html(URL, storage_options=headers)[0]
            symbols = df['Symbol'].dropna().tolist()
            print(f"Finished getting the {asset_type} symbols")
            print(f"{asset_type} symbols:", symbols)
            return symbols
        elif asset_type == "crypto" and URL is None:
            print(f"Getting {asset_type} symbols (hardcoded)...")
            return ["BTC-USD", "ETH-USD"]

    @task
    def get_asset_info(asset_type, symbols):
        print("Connection to mongo ")
        mongoClient = mongo.MongoDBClient(
            uri=MONGO_DB_URI,
            database="raw_data_db",
            collection="ingestion"
        )
        mongoClient.connect()
        print("connected to mongo ")
        print("fetching info for symbols:", symbols)
        print("symbols type:", type(symbols))
        ids = []
        for symbol in symbols : 
            print(f"Fetching info for symbol: {symbol}")
            ticker = yf.Ticker(symbol)
            info = ticker.info
            info["symbol"] = symbol
            id = asset_type + "_" + symbol + "_info"
            ids.append(id)
            doc = {
                "_id" : id,
                "type": asset_type,
                "data": info,
                "extracted_at": datetime.now(),                
            }
            mongoClient.write(doc)
        mongoClient.disconnect()
        print("list of ids:", ids)#TODO remove
        return ids

    @task
    def get_asset_history(asset_type, symbols):
        print("getting the history for symbols:", symbols)
        mongoClient = mongo.MongoDBClient(
            uri=MONGO_DB_URI,
            database="raw_data_db",
            collection="ingestion"
        )
        mongoClient.connect()
        ids = []
        for symbol in symbols : 
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=f"{20*12}mo", interval="1mo") 
            hist = hist.reset_index()
            hist = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
            id = asset_type + "_" + symbol + "_history"
            ids.append(id)
            doc = {
                "_id" : id,
                "type": asset_type,
                "data": hist,
                "extracted_at": datetime.now(),
            }
            mongoClient.write(doc)
        mongoClient.disconnect()
        return ids
    
    @task 
    def populate_redis_queue(data, queue_name):
        redisClient = redis.RedisClient(uri=REDIS_URI)
        print("ids to enqueue:", data) # TODO remove
        redisClient.connect()
        if isinstance(data, (list, tuple, set)):
            for id in data:
                redisClient.write(queue_name, id)
        else:
            redisClient.write(queue_name, data)
        redisClient.disconnect()

    @task
    def download_file(URL, path):
        response = requests.get(URL, stream=True)
        response.raise_for_status()
        # Ensure the download path exists
        os.makedirs(path, exist_ok=True)
        # Get the filename from the URL or use a default
        filename = os.path.basename(URL)
        if not filename.endswith('.zip'):
            filename = 'downloaded_file.zip'
        file_path = os.path.join(path, filename)
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded file from {URL} to {file_path}")
        return file_path

    @task 
    def unzip_file(zip_file_path, extract_to_path):
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to_path)
            extracted_files = zip_ref.namelist()
        print(f"Extracted {zip_file_path} to {extract_to_path}")
        # Delete the zip file after extraction
        try:
            os.remove(zip_file_path)
            print(f"Deleted zip file: {zip_file_path}")
        except Exception as e:
            print(f"Failed to delete zip file: {zip_file_path}. Error: {e}")
        # Return the absolute path of the first extracted file (or None if nothing extracted)
        if extracted_files:
            abs_path = os.path.abspath(os.path.join(extract_to_path, extracted_files[0]))
            print(f"Extracted file absolute path: {abs_path}")
            return abs_path
        else:
            return None
    

    # ==========================
    # SYMBOLS EXTRACTION TASKS
    # ==========================
    crypto_symbols = get_asset_symbols("crypto", URL=None)
    forex_symbols = get_asset_symbols("forex", URL=FOREX_SYMBOLS_SCRAPER_URL)
    futures_symbols = get_asset_symbols("futures", URL=FUTURES_SYMBOLS_SCRAPER_URL)
    indices_symbols = get_asset_symbols("indices", URL=INDICES_SYMBOLS_SCRAPER_URL)

    # chunking
    crypto_chunks = chunk_list(crypto_symbols, 5)
    forex_chunks = chunk_list(forex_symbols, 5)
    futures_chunks = chunk_list(futures_symbols, 5)
    indices_chunks = chunk_list(indices_symbols, 5)

    end_task = end()

    # ==========================
    # INFO EXTRACTION TASKS
    # ==========================
    crypto_info_keys = get_asset_info.partial(asset_type="crypto").expand(symbols=crypto_chunks)
    forex_info_keys = get_asset_info.partial(asset_type="forex").expand(symbols=forex_chunks)
    futures_info_keys = get_asset_info.partial(asset_type="futures").expand(symbols=futures_chunks)
    indices_info_keys = get_asset_info.partial(asset_type="indices").expand(symbols=indices_chunks)
    populate_redis_queue.partial(queue_name="crypto_info_queue").expand(data=crypto_info_keys) >> end_task
    populate_redis_queue.partial(queue_name="forex_info_queue").expand(data=forex_info_keys) >> end_task
    populate_redis_queue.partial(queue_name="futures_info_queue").expand(data=futures_info_keys) >> end_task
    populate_redis_queue.partial(queue_name="indices_info_queue").expand(data=indices_info_keys) >> end_task    

    crypto_history_keys = get_asset_history.partial(asset_type="crypto").expand(symbols=crypto_chunks)
    forex_history_keys = get_asset_history.partial(asset_type="forex").expand(symbols=forex_chunks)
    futures_history_keys = get_asset_history.partial(asset_type="futures").expand(symbols=futures_chunks)
    indices_history_keys = get_asset_history.partial(asset_type="indices").expand(symbols=indices_chunks)

    populate_redis_queue.partial(queue_name="crypto_history_queue").expand(data=crypto_history_keys) >> end_task
    populate_redis_queue.partial(queue_name="forex_history_queue").expand(data=forex_history_keys) >> end_task
    populate_redis_queue.partial(queue_name="futures_history_queue").expand(data=futures_history_keys) >> end_task
    populate_redis_queue.partial(queue_name="indices_history_queue").expand(data=indices_history_keys) >> end_task

    # ==========================
    # EVENTS EXTRACTION TASKS
    # ==========================
    file_path = download_file(WORLDWIDE_EVENTS_CSV_FILE_URL, SHARED_FOLDER_PATH_AIRFLOW)
    extracted_path = unzip_file(file_path, SHARED_FOLDER_PATH_AIRFLOW)
    populate_redis_queue(extracted_path, "worldwide_events_file_queue") >> end_task

    # ==========================
    # TASK DEPENDENCIES
    # ==========================

    init_env() >> [ 
        crypto_symbols,
        forex_symbols,
        futures_symbols,
        indices_symbols, 
        file_path
        ]


ingestion_pipeline()
