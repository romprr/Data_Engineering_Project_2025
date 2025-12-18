from datetime import datetime, timedelta

from airflow.decorators import dag, task
import yfinance as yf
import pandas as pd
import utils.mongo as mongo
import utils.redis as redis
import os
import logging
import requests
import json
import zipfile

# =========================
# LOGGING CONFIGURATION
# =========================
yf.set_tz_cache_location("/tmp/yfinance_cache")
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# =========================
# ENV VARIABLES IMPORTS
# ========================= 
INDICES_SYMBOLS_SCRAPER_URL = os.getenv("INDICES_SYMBOLS_SCRAPER_URL")
FUTURES_SYMBOLS_SCRAPER_URL = os.getenv("FUTURES_SYMBOLS_SCRAPER_URL")
FOREX_SYMBOLS_SCRAPER_URL = os.getenv("FOREX_SYMBOLS_SCRAPER_URL")
WORLDWIDE_EVENTS_CSV_FILE_URL = os.getenv("WORLDWIDE_EVENTS_CSV_FILE_URL")
SHARED_FOLDER_PATH_AIRFLOW = os.getenv("SHARED_FOLDER_PATH_AIRFLOW")
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
    def start():
        """Empty start task"""
        print("Starting the ingestion pipeline...")

    @task
    def chunk_list(symbols, chunk_size):
        """Chunk a list into smaller lists of a specified size."""
        return [
            symbols[i : i + chunk_size]
            for i in range(0, len(symbols), chunk_size)
        ]

    @task
    def get_asset_symbols(asset_type, URL):
        """Scrapper to get the asset symbols from a given URL or hardcoded for crypto."""
        print(f"Starting to get the {asset_type} symbols...")
        symbols = []
        if URL:
            print(f"Scrapping: {URL}")
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            df = pd.read_html(URL, storage_options=headers)[0]
            symbols = df['Symbol'].dropna().tolist()
            print(f"Scrapping completed.")
        elif asset_type == "crypto" and URL is None:
            print(f"Getting {asset_type} symbols (hardcoded)...")
            symbols = ["BTC-USD", "ETH-USD"]
        print(f"Number of {asset_type} symbols found: {len(symbols)}")
        return symbols

    @task
    def get_asset_info(asset_type, symbols):
        """Get asset info from yfinance and store in MongoDB. 
        The MongoDB document ID is formatted as {asset_type}_{symbol}_info 
        to be later pushed into redis with other metadata."""
        print(f"Fetching info for asset type: {asset_type}")
        mongoClient = mongo.MongoDBClient(
            uri=MONGO_DB_URI,
            database=MONGO_DB_INGESTION_DATABASE,
            collection=MONGO_DB_INGESTION_COLLECTION
        )
        mongoClient.connect()
        metadatas = []
        for symbol in symbols : 
            print(f"Fetching info for symbol: {symbol}")
            ticker = yf.Ticker(symbol)
            info = ticker.info
            info["symbol"] = symbol
            id = asset_type + "_" + symbol + "_info"
            metadata = {
                "_id" : id,
                "type": asset_type,
                "extracted_at": datetime.now().isoformat(),                
            }
            metadatas.append(metadata)
            doc = {
                **metadata,
                "data": info,
            }
            mongoClient.write(doc)
        mongoClient.disconnect()
        print(f"Completed fetching info for asset type: {asset_type}")
        return metadatas

    @task
    def get_asset_history(asset_type, symbols):
        """Get asset historical data from yfinance and store in MongoDB. 
        The MongoDB document ID is formatted as {asset_type}_{symbol}_history
        to be later pushed into redis with other metadata."""
        print(f"Fetching history for asset type: {asset_type}")
        mongoClient = mongo.MongoDBClient(
            uri=MONGO_DB_URI,
            database=MONGO_DB_INGESTION_DATABASE,
            collection=MONGO_DB_INGESTION_COLLECTION
        )
        mongoClient.connect()
        metadatas = []
        for symbol in symbols : 
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=f"{20*12}mo", interval="1mo") 
            hist = hist.reset_index()
            hist = json.loads(hist.to_json(orient="records", date_format="epoch", date_unit="s"))
            id = asset_type + "_" + symbol + "_history"
            metadata = {
                "_id" : id,
                "type": asset_type,
                "extracted_at": datetime.now().isoformat(),
            }
            metadatas.append(metadata)
            doc = {
                **metadata,
                "data": hist,
            }
            mongoClient.write(doc)
        mongoClient.disconnect()
        print(f"Completed fetching history for asset type: {asset_type}")
        return metadatas
    
    @task 
    def populate_redis_queue(data, queue_name):
        """Pushing mongo document metadata into Redis queue for further processing."""
        print(f"Pushing data into Redis queue: {queue_name}")
        redisClient = redis.RedisClient(uri=REDIS_1_URI)
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
            return {
                "type" : data_type,
                "data" : abs_path, 
                "extracted_at": datetime.now().isoformat(),
            }
        else:
            return None
    
    @task
    def end():
        """Empty end task"""
        print("Ingestion pipeline completed")
    
    # SYMBOLS EXTRACTION
    crypto_symbols = get_asset_symbols("crypto", URL=None)
    forex_symbols = get_asset_symbols("forex", URL=FOREX_SYMBOLS_SCRAPER_URL)
    futures_symbols = get_asset_symbols("futures", URL=FUTURES_SYMBOLS_SCRAPER_URL)
    indices_symbols = get_asset_symbols("indices", URL=INDICES_SYMBOLS_SCRAPER_URL)

    # CHUNKING SYMBOLS FOR PARALLEL PROCESSING
    crypto_chunks = chunk_list(crypto_symbols, 5)
    forex_chunks = chunk_list(forex_symbols, 5)
    futures_chunks = chunk_list(futures_symbols, 5)
    indices_chunks = chunk_list(indices_symbols, 5)

    # ASSET INFO AND HISTORY EXTRACTION
    crypto_info_metadata = get_asset_info.partial(asset_type="crypto").expand(symbols=crypto_chunks)
    forex_info_metadata = get_asset_info.partial(asset_type="forex").expand(symbols=forex_chunks)
    futures_info_metadata = get_asset_info.partial(asset_type="futures").expand(symbols=futures_chunks)
    indices_info_metadata = get_asset_info.partial(asset_type="indices").expand(symbols=indices_chunks)

    crypto_history_metadata = get_asset_history.partial(asset_type="crypto").expand(symbols=crypto_chunks)
    forex_history_metadata = get_asset_history.partial(asset_type="forex").expand(symbols=forex_chunks)
    futures_history_metadata = get_asset_history.partial(asset_type="futures").expand(symbols=futures_chunks)
    indices_history_metadata = get_asset_history.partial(asset_type="indices").expand(symbols=indices_chunks)

    # CSV FILE DOWNLOAD AND EXTRACTION
    file_path = download_file(URL=WORLDWIDE_EVENTS_CSV_FILE_URL, path=SHARED_FOLDER_PATH_AIRFLOW)
    extracted_metadata = unzip_file(data_type="worldwide_events", zip_file_path=file_path, extract_to_path=SHARED_FOLDER_PATH_AIRFLOW)

    populate_tasks = [
        populate_redis_queue.partial(queue_name=CRYPTO_INFO_QUEUE).expand(data=crypto_info_metadata),
        populate_redis_queue.partial(queue_name=FOREX_INFO_QUEUE).expand(data=forex_info_metadata),
        populate_redis_queue.partial(queue_name=FUTURES_INFO_QUEUE).expand(data=futures_info_metadata),
        populate_redis_queue.partial(queue_name=INDICES_INFO_QUEUE).expand(data=indices_info_metadata),
        populate_redis_queue.partial(queue_name=CRYPTO_HISTORY_QUEUE).expand(data=crypto_history_metadata),
        populate_redis_queue.partial(queue_name=FOREX_HISTORY_QUEUE).expand(data=forex_history_metadata),
        populate_redis_queue.partial(queue_name=FUTURES_HISTORY_QUEUE).expand(data=futures_history_metadata),
        populate_redis_queue.partial(queue_name=INDICES_HISTORY_QUEUE).expand(data=indices_history_metadata),
        populate_redis_queue(queue_name=FILES_QUEUE, data=extracted_metadata)
    ]

    # TASK DEPENDENCIES
    start() >> [ 
        crypto_symbols,
        forex_symbols,
        futures_symbols,
        indices_symbols, 
        file_path
        ]
    populate_tasks >> end()

ingestion_pipeline()