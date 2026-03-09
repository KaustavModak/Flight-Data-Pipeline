import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow.decorators import dag, task

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingestion
from scripts.silver_transform import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate
from scripts.data_quality import check_gold_data
from scripts.load_to_postgres import load_gold_to_postgres

@dag(
    dag_id="flights_ops_medallion_pipe",
    start_date=datetime(2026, 1, 7),
    schedule="*/30 * * * *",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1)
    }
)
def flights_pipeline():

    @task
    def bronze_ingest(**kwargs):
        run_bronze_ingestion(**kwargs)

    @task
    def silver_transform(**kwargs):
        run_silver_transform(**kwargs)

    @task
    def gold_aggregate(**kwargs):
        run_gold_aggregate(**kwargs)

    @task
    def data_quality(**kwargs):
        check_gold_data(**kwargs)

    @task
    def load_to_postgres(**kwargs):
        load_gold_to_postgres(**kwargs)

    bronze=bronze_ingest()
    silver=silver_transform()
    gold=gold_aggregate()
    quality=data_quality()
    load=load_to_postgres()

    bronze >> silver >> gold >> quality >> load


flights_pipeline()