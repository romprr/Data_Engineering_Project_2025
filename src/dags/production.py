from datetime import datetime, timedelta
import os
import json
import pandas as pd
from utils.db_clients import PGDriver, create_sql_operator
from utils.staging_formatter import AssetHistoryFormatters, AssetInfosFormatters
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Postgres env variables 
POSTGRES_CRED_USER=os.getenv('POSTGRES_USER')
POSTGRES_CRED_PASSWORD=os.getenv('POSTGRES_PASSWORD')
POSTGRES_CRED_HOST=os.getenv("POSTGRES_HOST")
POSTGRES_CRED_DB=os.getenv('POSTGRES_DB')

# Prod variables
SQL_FOLDER = os.getenv('SQL_PRODUCTION_FOLDER')

default_args = {
    "owner" : "niceJobTeam",
    "depends_on_past" : False,
    "retries" : 3,
    "retry_delay" : timedelta(minutes=5),
    "email_on_failure" : False
}

@dag(
    dag_id="production_pipeline",
    default_args=default_args,
    schedule=[Dataset('redis://production_data_ready')],
    start_date=datetime.now() - timedelta(days=1),
    description="The pipeline that will organize data in an OLAP schema.",
    catchup=False,
    tags=["production"],
    template_searchpath=[f'/opt/airflow/dags/{SQL_FOLDER}/']
)


def production_pipeline() :
    
    @task
    def start() :
        print("Starting the production pipeline...")

    
    load_asset_info = create_sql_operator(
        task_id="load_asset_info",
        sql_file_name="dim_asset_info.sql"
    )

    load_conflict_info = create_sql_operator(
        task_id="load_conflict_info",
        sql_file_name="dim_conflict_info.sql"
    )

    load_conflict_actors = create_sql_operator(
        task_id="load_conflict_actors",
        sql_file_name="dim_conflict_actor.sql"
    )

    load_date_dimension = create_sql_operator(
        task_id="load_date_dim",
        sql_file_name="dim_month_date.sql"
    )

    # load_episodes = create_sql_operator(
    #     task_id="load_conflict_episodes",
    #     sql_file_name="fact_conflict_episode.sql"
    # )

    load_sides = create_sql_operator(
        task_id="load_conflict_sides",
        sql_file_name="bridge_conflict_side.sql"
    )

    load_asset_values = create_sql_operator(
        task_id="load_asset_values",
        sql_file_name="fact_asset_value.sql"
    )

    # load_episode_date = create_sql_operator(
    #     task_id="load_episode_date",
    #     sql_file_name="bridge_episode_date.sql"
    # )

    load_region = create_sql_operator(
        task_id="load_regions",
        sql_file_name="dim_region.sql"
    )

    load_dim_episodes = create_sql_operator(
        task_id="load_dim_episodes",
        sql_file_name="dim_conflict_episode.sql"
    )

    load_fact_episode_monthly = create_sql_operator(
        task_id="load_fact_episode_monthly",
        sql_file_name="fact_conflict_episode_monthly.sql"
    )

    load_fact_monthly_country_status = create_sql_operator(
        task_id="load_fact_monthly_country_status",
        sql_file_name="fact_monthly_country_status.sql"
    )

    @task()
    def end():
        print("Production pipeline ended")

    start() >> [
        load_asset_info, 
        load_conflict_info, 
        load_conflict_actors,
        load_date_dimension,
    ]
    
    [load_dim_episodes, load_region, load_date_dimension] >> load_fact_episode_monthly
    [load_sides, load_fact_episode_monthly, load_conflict_actors] >> load_fact_monthly_country_status

    [load_conflict_info, load_asset_info] >> load_region >> load_dim_episodes
    [load_asset_info, load_date_dimension, load_region] >> load_asset_values
    
    [load_dim_episodes, load_conflict_actors] >> load_sides

    [load_asset_values, load_sides, load_fact_monthly_country_status] >> end()

production_pipeline()