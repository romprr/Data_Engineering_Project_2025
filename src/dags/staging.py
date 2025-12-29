from datetime import datetime, timedelta
import os
import json
import pandas as pd
# from utils.dbt_interface import run_dbt_model
from utils.db_clients import PGDriver, PGQueries
from utils.staging_formatter import AssetHistoryFormatters, AssetInfosFormatters
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from utils.mongo import MongoDBClient
from utils.redis import RedisClient

production_ready = Dataset('redis://production_data_ready')

# Env variables from ingestion
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
SHARED_FOLDER_PATH_POSTGRES = os.getenv('SHARED_FOLDER_PATH_POSTGRES')

# Postgres env variables 
POSTGRES_CRED_USER=os.getenv('POSTGRES_USER')
POSTGRES_CRED_PASSWORD=os.getenv('POSTGRES_PASSWORD')
POSTGRES_CRED_HOST=os.getenv("POSTGRES_HOST")
POSTGRES_CRED_DB=os.getenv('POSTGRES_DB')

# Env variables specific to staging
REDIS_2_URI = os.getenv("REDIS_2_URI")

SQL_FOLDER = os.getenv("SQL_STAGING_FOLDER")

SQL_ASSETS_PATH = 'assets'
SQL_UCDP_PATH = 'ucdp'


def create_sql_operator(task_id : str, dag_path : str, sql_file_name : str) -> SQLExecuteQueryOperator :
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id="postgres",
        sql=os.path.join(dag_path, sql_file_name)
    )
    

default_args = {
    "owner" : "niceJobTeam",
    "depends_on_past" : False,
    "retries" : 3,
    "retry_delay" : timedelta(minutes=5),
    "email_on_failure" : False
}

@dag(
    dag_id="staging_pipeline",
    default_args=default_args,
    schedule=[Dataset('redis://staging_data_ready')],
    start_date=datetime.now() - timedelta(days=1),
    description="The pipeline that will transform and clean the data into usable information.",
    catchup=False,
    tags=["staging"],
    template_searchpath=[f'/opt/airflow/dags/{SQL_FOLDER}/']
)

def staging_pipeline() :
    """Staging DAG to load data from MongoDB & clean it"""
    
    @task
    def start() :
        print("Starting the staging pipeline...")
    
    @task
    def load_asset_infos_data(format_helper, query, queue_name) :
        # Connect to Redis
        redisClient = RedisClient(REDIS_1_URI)
        redisClient.connect()

        # Connect to MongoDB
        mongoClient = MongoDBClient(
            uri=MONGO_DB_URI,
            database=MONGO_DB_INGESTION_DATABASE,
            collection=MONGO_DB_INGESTION_COLLECTION
        )

        # Connect to postgres
        postgresClient = PGDriver(
            POSTGRES_CRED_USER,
            POSTGRES_CRED_PASSWORD,
            POSTGRES_CRED_HOST,
            POSTGRES_CRED_DB
        )

        mongoClient.connect()

        redis_val = redisClient.read(queue_name)
        while redis_val != None :
            raw_json = json.loads(redis_val)
            collection_id = raw_json["_id"]
            raw_data = mongoClient.read({"_id" : collection_id})[0]

            processed_info : pd.DataFrame = format_helper(raw_data["data"])
            postgresClient.insert_dataframe(processed_info, query)
            redis_val = redisClient.read(queue_name)
        mongoClient.disconnect()
        

    @task
    def load_asset_history_data(format_helper, query, queue_name) :
        # Connect to Redis
        redisClient = RedisClient(REDIS_1_URI)
        redisClient.connect()

        # Connect to MongoDB
        mongoClient = MongoDBClient(
            uri=MONGO_DB_URI,
            database=MONGO_DB_INGESTION_DATABASE,
            collection=MONGO_DB_INGESTION_COLLECTION
        )

        # Connect to postgres
        postgresClient = PGDriver(
            POSTGRES_CRED_USER,
            POSTGRES_CRED_PASSWORD,
            POSTGRES_CRED_HOST,
            POSTGRES_CRED_DB
        )

        mongoClient.connect()

        redis_val = redisClient.read(queue_name)
        while redis_val != None :
            raw_json = json.loads(redis_val)
            collection_id = raw_json["_id"]
            raw_data = mongoClient.read({"_id" : collection_id}, {"_id" : 1, "data" : 1})[0]

            processed_info : pd.DataFrame = format_helper(raw_data["_id"], raw_data["data"])
            postgresClient.insert_dataframe(processed_info, query)
            redis_val = redisClient.read(queue_name)
        mongoClient.disconnect()

    @task(multiple_outputs=True)
    def fetch_multiple_files(queue_name) :
        redisClient = RedisClient(REDIS_1_URI)
        redisClient.connect()

        ucdp_yearly = []
        ucdp_actors = []

        redis_val = redisClient.read(queue_name)
        while redis_val != None :
            data = json.loads(redis_val)["data"]
            data_type = json.loads(redis_val)["type"]
            if data_type == 'worldwide_events' :
                ucdp_yearly.append(data)
            else :
                ucdp_actors.append(data)
            redis_val = redisClient.read(queue_name)
        
        return {
            "ucdp_yearly" : ucdp_yearly,
            "ucdp_actors" : ucdp_actors
        }

    
    @task
    def transform_ucdp_yearly(file_path) :
        """
        Transform the data (ucdp) file to fit the PGSQL table;
        This will be called by the sql query operator;
        The task will return the file name  + path.
        """

        df = pd.read_csv(file_path, sep=',')

        df = df[
            ['conflict_id', 'location', 'year', 'side_a', 'side_b', 
            'side_a_2nd', 'side_b_2nd', 'gwno_a', 'gwno_b', 
            'gwno_b_2nd', 'gwno_a_2nd', 'region', 'intensity_level', 
            'cumulative_intensity', 'type_of_conflict', 'incompatibility', 
            'territory_name', 'start_date', 'start_date2', 'ep_end_date'
        ]]

        df.to_csv(file_path, index=False, na_rep='')

        f_path = file_path.rsplit('/',1)[-1]

        return {
            "file_path" : f'{SHARED_FOLDER_PATH_POSTGRES}/{f_path}'
        }


    @task
    def transform_ucdp_actors(file_path) :
        """
        Transform the data (ucdp) file to fit the PGSQL table;
        This will be called by the sql query operator;
        The task will return the file name  + path.
        """

        df = pd.read_csv(file_path, sep=',')

        df = df[
            ['ActorId', 'NameData', 'NameOrigFull', 'ConflictId']
        ]

        df = df.rename(columns={
            "ActorId" : "actor_id",
            "NameData" : "actor_name",
            "NameOrigFull" : "actor_og_name",
            "ConflictId" : "conflict_ids"
        })

        df.to_csv(file_path, index=False, na_rep='')

        f_path = file_path.rsplit('/',1)[-1]

        return {
            "file_path" : f'{SHARED_FOLDER_PATH_POSTGRES}/{f_path}'
        }


    # Assetss
    
    clean_forex_infos = create_sql_operator(
        task_id="clean_forex_infos",
        dag_path=SQL_ASSETS_PATH,
        sql_file_name="forex_info.sql"
    )

    clean_forex_values = create_sql_operator(
        task_id="clean_forex_values",
        dag_path=SQL_ASSETS_PATH,
        sql_file_name="forex_value.sql"
    )

    clean_index_infos = create_sql_operator(
        task_id="clean_index_infos",
        dag_path=SQL_ASSETS_PATH,
        sql_file_name="index_info.sql"
    )

    clean_index_values = create_sql_operator(
        task_id="clean_index_values",
        dag_path=SQL_ASSETS_PATH,
        sql_file_name="index_value.sql"
    )

    clean_futures_infos = create_sql_operator(
        task_id="clean_futures_infos",
        dag_path=SQL_ASSETS_PATH,
        sql_file_name="future_info.sql"
    )

    clean_futures_values = create_sql_operator(
        task_id="clean_futures_values",
        dag_path=SQL_ASSETS_PATH,
        sql_file_name="future_value.sql"
    )

    clean_crypto_infos = create_sql_operator(
        task_id="clean_crypto_infos",
        dag_path=SQL_ASSETS_PATH,
        sql_file_name="crypto_info.sql"
    )

    clean_crypto_values = create_sql_operator(
        task_id="clean_crypto_values",
        dag_path=SQL_ASSETS_PATH,
        sql_file_name="crypto_value.sql"
    )

    # UCDP

    clean_conflicts = create_sql_operator(
        task_id="clean_ucdp_conflicts",
        dag_path=SQL_UCDP_PATH,
        sql_file_name="ucdp_conflict.sql"
    )

    clean_episodes = create_sql_operator(
        task_id="clean_ucdp_episodes",
        dag_path=SQL_UCDP_PATH,
        sql_file_name="ucdp_episode.sql"
    )

    clean_side = create_sql_operator(
        task_id="clean_ucdp_side",
        dag_path=SQL_UCDP_PATH,
        sql_file_name="ucdp_side.sql"
    )

    clean_locations = create_sql_operator(
        task_id="clean_location",
        dag_path=SQL_UCDP_PATH,
        sql_file_name="ucdp_location.sql"
    )

    clean_conflict_locations = create_sql_operator(
        task_id="clean_ucdp_conflict_location",
        dag_path=SQL_UCDP_PATH,
        sql_file_name="ucdp_conflicts_location.sql"
    )

    clean_region = create_sql_operator(
        task_id="clean_ucdp_region",
        dag_path=SQL_UCDP_PATH,
        sql_file_name="ucdp_region.sql"
    )

    clean_conflict_regions = create_sql_operator(
        task_id="clean_ucd_conflicts_region",
        dag_path=SQL_UCDP_PATH,
        sql_file_name="ucdp_conflicts_region.sql"
    )

    clean_actors = create_sql_operator(
        task_id="clean_ucdp_actors",
        dag_path=SQL_UCDP_PATH,
        sql_file_name="ucdp_actors.sql"
    )

    @task
    def get_yearly_files(all_files):
        return all_files["ucdp_yearly"]

    @task
    def get_actor_files(all_files):
        return all_files["ucdp_actors"]

    # @task
    # def populate_redis_queue(data, queue_name) :
    #     pass
    
    @task(outlets=[production_ready])
    def end():
        print("Staging pipeline ended")
    
    

    

    load_crypto_infos = load_asset_infos_data(AssetInfosFormatters.format_crypto_infos, PGQueries.crypto_infos_insert, CRYPTO_INFO_QUEUE)
    load_forex_infos = load_asset_infos_data(AssetInfosFormatters.format_forex_infos, PGQueries.forex_infos_insert, FOREX_INFO_QUEUE)
    load_futures_infos = load_asset_infos_data(AssetInfosFormatters.format_futures_infos, PGQueries.future_infos_insert, FUTURES_INFO_QUEUE)
    load_index_infos = load_asset_infos_data(AssetInfosFormatters.format_index_infos, PGQueries.index_infos_insert, INDICES_INFO_QUEUE)


    load_crypto_history = load_asset_history_data(AssetHistoryFormatters.format_asset_history, PGQueries.crypto_values_insert, CRYPTO_HISTORY_QUEUE)
    load_forex_history = load_asset_history_data(AssetHistoryFormatters.format_asset_history, PGQueries.forex_values_insert, FOREX_HISTORY_QUEUE)
    load_futures_history = load_asset_history_data(AssetHistoryFormatters.format_asset_history, PGQueries.future_values_insert, FUTURES_HISTORY_QUEUE)
    load_index_history = load_asset_history_data(AssetHistoryFormatters.format_asset_history, PGQueries.index_values_insert, INDICES_HISTORY_QUEUE)

    files = fetch_multiple_files(FILES_QUEUE)


    start() >> [files, load_forex_infos, load_index_infos, load_crypto_infos, load_futures_infos]

    conflict_list = get_yearly_files(files)
    actors_list = get_actor_files(files)

    ucdp_conflict_path = transform_ucdp_yearly.expand(file_path=conflict_list)
    ucdp_actors_path = transform_ucdp_actors.expand(file_path=actors_list)


    load_ucdp_conflicts = SQLExecuteQueryOperator.partial(
        task_id="load_ucdp_conflicts",
        conn_id="postgres",
        sql="""
TRUNCATE TABLE raw.CONFLICT;

COPY raw.CONFLICT
FROM %(file_path)s
DELIMITER ','
CSV HEADER;
"""
    ).expand(
        parameters=ucdp_conflict_path
    )

    load_ucdp_actors = SQLExecuteQueryOperator.partial(
        task_id="load_ucdp_actors",
        conn_id="postgres",
        sql="""
TRUNCATE TABLE raw.UCDP_ACTORS;

COPY raw.UCDP_ACTORS
FROM %(file_path)s
DELIMITER ','
CSV HEADER;
"""
    ).expand(
        parameters=ucdp_actors_path
    )


    load_futures_infos >> load_futures_history >> clean_futures_infos >> clean_futures_values
    load_index_infos >> load_index_history >> clean_index_infos >> clean_index_values
    load_crypto_infos >> load_crypto_history >> clean_crypto_infos >> clean_crypto_values
    load_forex_infos >> load_forex_history >> clean_forex_infos >> clean_forex_values
    [load_ucdp_conflicts, load_ucdp_actors] >> clean_conflicts >> clean_episodes >> clean_actors >> clean_side >> [clean_locations, clean_region, clean_conflict_locations, clean_conflict_regions]
    
    [
        clean_futures_values, 
        clean_index_values, 
        clean_crypto_values, 
        clean_forex_values,
        clean_locations, 
        clean_region, 
        clean_conflict_locations, 
        clean_conflict_regions 
    ] >> end()

staging_pipeline()