from datetime import datetime, timedelta
from unittest import case

from airflow.decorators import dag, task
from airflow.datasets import Dataset
import yfinance as yf
import pandas as pd
import utils.mongo as mongo
import utils.redis as redis
import os
import logging
import requests
import json
import socket
import gc
import zipfile

# =========================
# LOGGING CONFIGURATION
# =========================
yf.set_tz_cache_location("/tmp/yfinance_cache")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# =========================
# ENV VARIABLES IMPORTS
# ========================= 
# URLS
INDICES_SYMBOLS_SCRAPER_URL = os.getenv("INDICES_SYMBOLS_SCRAPER_URL")
FUTURES_SYMBOLS_SCRAPER_URL = os.getenv("FUTURES_SYMBOLS_SCRAPER_URL")
FOREX_SYMBOLS_SCRAPER_URL = os.getenv("FOREX_SYMBOLS_SCRAPER_URL")
WORLDWIDE_EVENTS_CSV_FILE_URL = os.getenv("WORLDWIDE_EVENTS_CSV_FILE_URL")
WORLDWIDE_ACTORS_CSV_FILE_URL = os.getenv("WORLDWIDE_ACTORS_CSV_FILE_URL")
WORLDWIDE_GEOREFERENCE_CSV_FILE_URL = os.getenv("WORLDWIDE_GEOREFERENCE_CSV_FILE_URL")

# PATHS
SHARED_FOLDER_PATH_AIRFLOW = os.getenv("SHARED_FOLDER_PATH_AIRFLOW")
OFFLINE_CRYPTOCURRENCIES_INFO=os.getenv("OFFLINE_CRYPTOCURRENCIES_INFO")
OFFLINE_FOREX_INFO=os.getenv("OFFLINE_FOREX_INFO")
OFFLINE_FUTURES_INFO=os.getenv("OFFLINE_FUTURES_INFO")
OFFLINE_INDICES_INFO=os.getenv("OFFLINE_INDICES_INFO")
OFFLINE_CRYPTOCURRENCIES_HISTORY=os.getenv("OFFLINE_CRYPTOCURRENCIES_HISTORY")
OFFLINE_FOREX_HISTORY=os.getenv("OFFLINE_FOREX_HISTORY")
OFFLINE_FUTURES_HISTORY=os.getenv("OFFLINE_FUTURES_HISTORY")
OFFLINE_INDICES_HISTORY=os.getenv("OFFLINE_INDICES_HISTORY")
OFFLINE_WORLDWIDE_EVENTS=os.getenv("OFFLINE_WORLDWIDE_EVENTS")
OFFLINE_WORLDWIDE_ACTORS=os.getenv("OFFLINE_WORLDWIDE_ACTORS")
OFFLINE_WORLDWIDE_GEO=os.getenv("OFFLINE_WORLDWIDE_GEO")

# DATABASES AND QUEUES 
MONGO_DB_INGESTION_COLLECTION = os.getenv("MONGO_DB_INGESTION_COLLECTION")
MONGO_DB_INGESTION_DATABASE = os.getenv("MONGO_DB_INGESTION_DATABASE")
MONGO_DB_URI = os.getenv("MONGO_DB_URI")
REDIS_1_URI = os.getenv("REDIS_1_URI")
CRYPTO_INFO_QUEUE = os.getenv("CRYPTO_INFO_QUEUE")
FOREX_INFO_QUEUE = os.getenv("FOREX_INFO_QUEUE")
FUTURES_INFO_QUEUE = os.getenv("FUTURES_INFO_QUEUE")
INDICES_INFO_QUEUE = os.getenv("INDICES_INFO_QUEUE")
CRYPTO_HISTORY_QUEUE = os.getenv("CRYPTO_HISTORY_QUEUE")
FOREX_HISTORY_QUEUE = os.getenv("FOREX_HISTORY_QUEUE")
FUTURES_HISTORY_QUEUE = os.getenv("FUTURES_HISTORY_QUEUE")
INDICES_HISTORY_QUEUE = os.getenv("INDICES_HISTORY_QUEUE")
FILES_QUEUE = os.getenv("FILES_QUEUE")

# Dataset to signal staging can trigger
staging_reagy = Dataset('redis://staging_data_ready')

# =========================
# UTILS FUNCTIONS
# =========================
def build_metadata(symbol, asset_type, information_type):
    """Build metadata dictionary for an asset."""
    print(f"Building metadata for {asset_type} - {symbol} - {information_type}")
    metadata = {
        "_id" : f"{asset_type}_{symbol}_{information_type}",
        "type": asset_type,
        "symbol": symbol,
        "information_type": information_type,
        "extracted_at": datetime.now().isoformat(),
    }
    print(f"Built metadata: {metadata}")
    return metadata

def get_mongo_client():
    """Create and return a MongoDB client."""
    print("Creating MongoDB client...")
    mongoClient = mongo.MongoDBClient(
        uri=MONGO_DB_URI,
        database=MONGO_DB_INGESTION_DATABASE,
        collection=MONGO_DB_INGESTION_COLLECTION
    )
    print("MongoDB client created.")
    return mongoClient

def get_redis_client():
    """Create and return a Redis client."""
    print("Creating Redis client...")
    redisClient = redis.RedisClient(uri=REDIS_1_URI)
    print("Redis client created.")
    return redisClient

# =========================
# DAG DEFINITION
# =========================
default_args = {
    "owner": "niceJobTeam",
    "depends_on_past": False, # do not depend on past runs
    "retries": 3, # number of retries on failure
    "retry_delay": timedelta(minutes=5), # wait time between retries
    "email_on_failure": False, # disable email on failure
}

# Dataset to signal staging can trigger
staging_reagy = Dataset('redis://staging_data_ready')

@dag(
    dag_id="ingestion_pipeline",
    default_args=default_args, 
    schedule=None, 
    start_date=datetime.now() - timedelta(days=1), 
    description="The pipeline that will get the data from different sources and insert it into the Mongo database",
    catchup=False, 
    tags=["ingestion"],
    max_active_tasks=10  # Limit concurrent tasks to prevent OOM
    )

def ingestion_pipeline():
    """Ingestion DAG to extract data from various sources and load into MongoDB"""

    @task.branch
    def start():
        """Start of the dag, checks if the dag will run in online or offline mode."""
        print("Starting the ingestion pipeline...")
        try:
            socket.create_connection(("1.1.1.1", 443), timeout=5)
            print("Internet connection available.")
            return "is_online"
        except Exception as ex:
            print(f"No internet connection: {ex}")
            return "is_offline"
  
    @task(task_id="is_online")
    def is_online():
        """Empty task to start online mode"""
        print("Running in online mode.")

    
    @task(task_id="is_offline") 
    def is_offline():
        """Empty task to start offline mode"""
        print("Running in offline mode.")

    @task
    def chunk_list(symbols, chunk_size):
        """Chunk a list into smaller lists of a specified size."""
        return [
            symbols[i : i + chunk_size]
            for i in range(0, len(symbols), chunk_size)
        ]

    @task
    def fetch_asset_symbols(asset_type, URL):
        """Scrapper to get the asset symbols from a given URL or hardcoded for crypto."""
        print(f"Getting the {asset_type} symbols...")
        symbols = []
        if URL: # online scrapper not working 
            print(f"Scrapping: {URL}")
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            df = pd.read_html(URL, storage_options=headers)[0]
            symbols = df['Symbol'].dropna().tolist()
            print(f"Scrapping completed.")
        else:
            print(f"Getting {asset_type} symbols (hardcoded)...")
            match asset_type:
                case "crypto":
                    symbols = ["BTC-USD", "ETH-USD"]
                case "forex":
                    symbols = ['EURUSD=X', 'JPY=X', 'GBPUSD=X', 'AUDUSD=X', 'NZDUSD=X', 'EURJPY=X', 'GBPJPY=X', 'EURGBP=X', 'EURCAD=X', 'EURSEK=X', 'EURCHF=X', 'EURHUF=X', 'CNY=X', 'HKD=X', 'SGD=X', 'INR=X', 'MXN=X', 'PHP=X', 'IDR=X', 'THB=X', 'MYR=X', 'ZAR=X', 'RUB=X']
                case "futures":
                    symbols = ['ES=F', 'YM=F', 'NQ=F', 'RTY=F', 'ZB=F', 'ZN=F', 'ZF=F', 'ZT=F', 'GC=F', 'MGC=F', 'SI=F', 'SIL=F', 'PL=F', 'HG=F', 'PA=F', 'CL=F', 'HO=F', 'NG=F', 'RB=F', 'BZ=F', 'B0=F', 'ZC=F', 'ZO=F', 'KE=F', 'ZR=F', 'ZM=F', 'ZL=F', 'ZS=F', 'GF=F', 'HE=F', 'LE=F', 'CC=F', 'KC=F', 'CT=F', 'LBS=F', 'OJ=F', 'SB=F']
                case "indices":
                    symbols = ['^GSPC', '^DJI', '^IXIC', '^NYA', '^XAX', '^BUK100P', '^RUT', '^VIX', '^FTSE', '^GDAXI', '^FCHI', '^STOXX50E', '^N100', '^BFX', 'MOEX.ME', '^HSI', '^STI', '^AXJO', '^AORD', '^BSESN', '^JKSE', '^KLSE', '^NZ50', '^KS11', '^TWII', '^GSPTSE', '^BVSP', '^MXX', '^IPSA', '^MERV', '^TA125.TA', '^CASE30', '^JN0U.JO', 'DX-Y.NYB', '^125904-USD-STRD', '^XDB', '^XDE', '000001.SS', '^N225', '^XDN', '^XDA']
        print(f"Number of {asset_type} symbols found: {len(symbols)}")
        return symbols

    @task(pool="yfinance_pool", pool_slots=1)
    def query_yfinance_info(asset_type, symbols):
        """Query yfinance for asset information and store in MongoDB."""
        print(f"Fetching {asset_type} information...")
        mongoClient = get_mongo_client()
        mongoClient.connect()
        metadatas = []
        for symbol in symbols : 
            print(f"Fetching {symbol} information...")
            ticker = yf.Ticker(symbol)
            data = ticker.info
            data["symbol"] = symbol
            metadata = build_metadata(symbol, asset_type, "info")
            metadatas.append(metadata)
            doc = {
                **metadata,
                "data": data,
            }
            mongoClient.write(doc)
            del ticker, data, doc
            gc.collect()
        mongoClient.disconnect()
        print(f"Completed fetching {asset_type} information.")
        return metadatas
    
    @task(pool="yfinance_pool", pool_slots=1)
    def query_yfinance_history(asset_type, symbols):
        """Query yfinance for asset historical data and store in MongoDB."""
        print(f"Fetching {asset_type} historical data...")
        mongoClient = get_mongo_client()
        mongoClient.connect()
        metadatas = []
        for symbol in symbols : 
            print(f"Fetching {symbol} historical data...")
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=f"{20*12}mo", interval="1mo") # last 20 years monthly data
            data = data.reset_index()
            data = json.loads(data.to_json(orient="records", date_format="epoch", date_unit="s"))
            metadata = build_metadata(symbol, asset_type, "history")
            metadatas.append(metadata)
            doc = {
                **metadata,
                "data": data,
            }
            mongoClient.write(doc)
            del ticker, data, doc
            gc.collect()
        mongoClient.disconnect()
        print(f"Completed fetching {asset_type} historical data.")
        return metadatas
    
    @task
    def read_asset_files(asset_type, information_type, file_path):
        """Read asset data from offline files and store in MongoDB."""
        print(f"Reading offline file for {asset_type} - {information_type} from {file_path}...")
        mongoClient = get_mongo_client()
        mongoClient.connect()
        metadatas = []
        with open(file_path, 'r') as f:
            data = json.load(f)
            for item in data:
                symbol = item.get("symbol")
                metadata = build_metadata(symbol, asset_type, information_type)
                metadatas.append(metadata)
                if information_type == "history": 
                    item = item.get("history")
                doc = {
                    **metadata,
                    "data": item,
                }
                mongoClient.write(doc)
        mongoClient.disconnect()
        return metadatas

    @task 
    def populate_redis_queue(data, queue_name):
        """Pushing mongo document metadata into Redis queue for further processing."""
        print(f"Pushing data into Redis queue: {queue_name}")
        redisClient = get_redis_client()
        redisClient.connect()
        if isinstance(data, (list, tuple, set)):
            for metadata in data:
                redisClient.write(queue_name, json.dumps(metadata))
        else:
            redisClient.write(queue_name, json.dumps(data))
        redisClient.disconnect()
        print(f"Completed pushing data into Redis queue: {queue_name}")


    @task
    def download_file(URL, path):
        """Download a file from a URL to a specified path."""
        print(f"Downloading file from {URL} to {path}...")
        response = requests.get(URL, stream=True)
        response.raise_for_status()
        # Ensure the download path exists
        os.makedirs(path, exist_ok=True)
        # Get the filename from the URL or use a default
        filename = os.path.basename(URL)
        if not filename.endswith('.zip'):
            filename = 'downloaded_file.zip'
        file_path = os.path.join(path, filename)
        # Write the content to a file in chunks to handle large files
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded file from {URL} to {file_path}")
        return file_path

    @task 
    def unzip_file(data_type, zip_file_path, extract_to_path):
        """Unzip a zip file to a specified directory and delete the zip file after extraction."""
        print(f"Unzipping file {zip_file_path} to {extract_to_path}...")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to_path)
            extracted_files = zip_ref.namelist()
        print(f"Extracted {zip_file_path} to {extract_to_path}")
        
        # Return the absolute path of the first extracted file (or None if nothing extracted)
        if extracted_files:
            abs_path = os.path.abspath(os.path.join(extract_to_path, extracted_files[0]))
            print(f"Extracted file absolute path: {abs_path}")
            return {
                "type" : data_type,
                "data" : abs_path, 
                "extracted_at": datetime.now().isoformat(),
            }
        else:
            return None
        
    @task(outlets=[staging_reagy])
    def end():
        """Empty end task"""
        print("Ingestion pipeline completed")
    
    # ========================
    # TASK DEPENDENCIES
    # ========================
    # ONLINE
    # ------------------------
    # SYMBOLS EXTRACTION
    crypto_symbols = fetch_asset_symbols("crypto", URL=None)
    forex_symbols = fetch_asset_symbols("forex", URL=None)
    futures_symbols = fetch_asset_symbols("futures", URL=None)
    indices_symbols = fetch_asset_symbols("indices", URL=None)
    
    # CHUNKING SYMBOLS FOR PARALLEL PROCESSING
    crypto_chunks = chunk_list(crypto_symbols, 5)
    forex_chunks = chunk_list(forex_symbols, 5)
    futures_chunks = chunk_list(futures_symbols, 5)
    indices_chunks = chunk_list(indices_symbols, 5)

    # ASSET INFO AND HISTORY EXTRACTION
    crypto_info_metadata = query_yfinance_info.partial(asset_type="crypto").expand(symbols=crypto_chunks)
    forex_info_metadata = query_yfinance_info.partial(asset_type="forex").expand(symbols=forex_chunks)
    futures_info_metadata = query_yfinance_info.partial(asset_type="futures").expand(symbols=futures_chunks)
    indices_info_metadata = query_yfinance_info.partial(asset_type="indices").expand(symbols=indices_chunks)

    crypto_history_metadata = query_yfinance_history.partial(asset_type="crypto").expand(symbols=crypto_chunks)
    forex_history_metadata = query_yfinance_history.partial(asset_type="forex").expand(symbols=forex_chunks)
    futures_history_metadata = query_yfinance_history.partial(asset_type="futures").expand(symbols=futures_chunks)
    indices_history_metadata = query_yfinance_history.partial(asset_type="indices").expand(symbols=indices_chunks)

    # CSV FILE DOWNLOAD AND EXTRACTION
    file_path = download_file(URL=WORLDWIDE_EVENTS_CSV_FILE_URL, path=SHARED_FOLDER_PATH_AIRFLOW)
    file_metadata = unzip_file(data_type="worldwide_events", zip_file_path=file_path, extract_to_path=SHARED_FOLDER_PATH_AIRFLOW)

    file_path_actors = download_file(URL=WORLDWIDE_ACTORS_CSV_FILE_URL, path=SHARED_FOLDER_PATH_AIRFLOW)
    file_metadata_actors = unzip_file(data_type="worldwide_actors", zip_file_path=file_path, extract_to_path=SHARED_FOLDER_PATH_AIRFLOW)

    file_path_geo = download_file(URL=WORLDWIDE_GEOREFERENCE_CSV_FILE_URL, path=SHARED_FOLDER_PATH_AIRFLOW)
    file_metadata_geo = unzip_file(data_type="worldwide_geo", zip_file_path=file_path, extract_to_path=SHARED_FOLDER_PATH_AIRFLOW)

    # POPULATE REDIS TASKS
    populate_crypto_info = populate_redis_queue.partial(queue_name=CRYPTO_INFO_QUEUE).expand(data=crypto_info_metadata)
    populate_forex_info = populate_redis_queue.partial(queue_name=FOREX_INFO_QUEUE).expand(data=forex_info_metadata)
    populate_futures_info = populate_redis_queue.partial(queue_name=FUTURES_INFO_QUEUE).expand(data=futures_info_metadata)
    populate_indices_info = populate_redis_queue.partial(queue_name=INDICES_INFO_QUEUE).expand(data=indices_info_metadata)
    populate_crypto_history = populate_redis_queue.partial(queue_name=CRYPTO_HISTORY_QUEUE).expand(data=crypto_history_metadata)
    populate_forex_history = populate_redis_queue.partial(queue_name=FOREX_HISTORY_QUEUE).expand(data=forex_history_metadata)
    populate_futures_history = populate_redis_queue.partial(queue_name=FUTURES_HISTORY_QUEUE).expand(data=futures_history_metadata)
    populate_indices_history = populate_redis_queue.partial(queue_name=INDICES_HISTORY_QUEUE).expand(data=indices_history_metadata)
    populate_files = populate_redis_queue(queue_name=FILES_QUEUE, data=file_metadata)
    populate_files_actors = populate_redis_queue(queue_name=FILES_QUEUE, data=file_metadata_actors)
    populate_files_geo = populate_redis_queue(queue_name=FILES_QUEUE, data=file_metadata_geo)
    
    # OFFLINE 
    # ------------------------
    # ASSET INFO AND HISTORY EXTRACTION
    offline_crypto_info_metadata = read_asset_files(asset_type="crypto", information_type="info", file_path=OFFLINE_CRYPTOCURRENCIES_INFO)
    offline_forex_info_metadata = read_asset_files(asset_type="forex", information_type="info", file_path=OFFLINE_FOREX_INFO)
    offline_futures_info_metadata = read_asset_files(asset_type="futures", information_type="info", file_path=OFFLINE_FUTURES_INFO)
    offline_indices_info_metadata = read_asset_files(asset_type="indices", information_type="info", file_path=OFFLINE_INDICES_INFO)   
   
    offline_crypto_history_metadata = read_asset_files(asset_type="crypto", information_type="history", file_path=OFFLINE_CRYPTOCURRENCIES_HISTORY)
    offline_forex_history_metadata = read_asset_files(asset_type="forex", information_type="history", file_path=OFFLINE_FOREX_HISTORY)
    offline_futures_history_metadata = read_asset_files(asset_type="futures", information_type="history", file_path=OFFLINE_FUTURES_HISTORY)
    offline_indices_history_metadata = read_asset_files(asset_type="indices", information_type="history", file_path=OFFLINE_INDICES_HISTORY)
    
    # FILE EXTRACTION
    offline_file_metadata = unzip_file(data_type="worldwide_events", zip_file_path=OFFLINE_WORLDWIDE_EVENTS, extract_to_path=SHARED_FOLDER_PATH_AIRFLOW)
    offline_file_actors_metadata = unzip_file(data_type="worldwide_actors", zip_file_path=OFFLINE_WORLDWIDE_ACTORS, extract_to_path=SHARED_FOLDER_PATH_AIRFLOW)
    offline_file_geo_metadata = unzip_file(data_type="worldwide_geo", zip_file_path=OFFLINE_WORLDWIDE_GEO, extract_to_path=SHARED_FOLDER_PATH_AIRFLOW)

    # POPULATE REDIS TASKS
    populate_offline_crypto_info = populate_redis_queue(queue_name=CRYPTO_INFO_QUEUE, data=offline_crypto_info_metadata)
    populate_offline_forex_info = populate_redis_queue(queue_name=FOREX_INFO_QUEUE, data=offline_forex_info_metadata)
    populate_offline_futures_info = populate_redis_queue(queue_name=FUTURES_INFO_QUEUE, data=offline_futures_info_metadata)
    populate_offline_indices_info = populate_redis_queue(queue_name=INDICES_INFO_QUEUE, data=offline_indices_info_metadata)
    populate_offline_crypto_history = populate_redis_queue(queue_name=CRYPTO_HISTORY_QUEUE, data=offline_crypto_history_metadata)
    populate_offline_forex_history = populate_redis_queue(queue_name=FOREX_HISTORY_QUEUE, data=offline_forex_history_metadata)
    populate_offline_futures_history = populate_redis_queue(queue_name=FUTURES_HISTORY_QUEUE, data=offline_futures_history_metadata)
    populate_offline_indices_history = populate_redis_queue(queue_name=INDICES_HISTORY_QUEUE, data=offline_indices_history_metadata)
    populate_offline_files = populate_redis_queue(queue_name=FILES_QUEUE, data=offline_file_metadata)
    populate_offline_actors_files = populate_redis_queue(queue_name=FILES_QUEUE, data=offline_file_actors_metadata)
    populate_offline_geo_files = populate_redis_queue(queue_name=FILES_QUEUE, data=offline_file_geo_metadata)

    # ========================
    # TASK FLOW
    # ========================
    # BRANCHING
    online = is_online()
    offline = is_offline()
    start() >> [online, offline]
    
    # ONLINE PATH
    # ------------------------
    online >> [crypto_symbols, forex_symbols, futures_symbols, indices_symbols, file_path, file_path_actors, file_path_geo]
    [populate_crypto_info, populate_forex_info, populate_futures_info, populate_indices_info,
     populate_crypto_history, populate_forex_history, populate_futures_history, populate_indices_history,
     populate_files, populate_files_actors, populate_files_geo] >> end()
    
    # OFFLINE PATH
    # ------------------------
    offline >> [offline_crypto_info_metadata, offline_forex_info_metadata, offline_futures_info_metadata, 
                offline_indices_info_metadata, offline_crypto_history_metadata, offline_forex_history_metadata,
                offline_futures_history_metadata, offline_indices_history_metadata, offline_file_metadata, offline_file_actors_metadata, offline_file_geo_metadata]
    [populate_offline_crypto_info, populate_offline_forex_info, populate_offline_futures_info, 
     populate_offline_indices_info, populate_offline_crypto_history, populate_offline_forex_history,
     populate_offline_futures_history, populate_offline_indices_history, populate_offline_files, populate_offline_geo_files, populate_offline_actors_files] >> end()

ingestion_pipeline()