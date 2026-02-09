from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from src.agents.base import AgentContext
from src.agents.ingestion_agent import IngestionAgent
from src.agents.quality_agent import QualityAgent
from src.ops.validators import load_suite
from src.ops.config import ATHENA_DB_BRONZE

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def bronze_noaa_task(**context):
    run_id = context["run_id"]
    ctx = AgentContext(run_id=run_id, dag="bronze_ingestion_dag", dataset="noaa", layer="bronze")

    bucket = Variable.get("PLATFORM_S3_BUCKET")
    # Example: wherever NOAA raw landing is
    source_prefix = "hazard/bronze/noaa/"  # adjust to your actual prefix

    ing = IngestionAgent()
    if not ing.check_source_ready(ctx, source_prefix=source_prefix):
        raise RuntimeError(f"NOAA source prefix not ready: s3://{bucket}/{source_prefix}")

    job = Variable.get("GLUE_JOB_NOAA_BRONZE")
    ing.trigger_bronze_job(ctx, glue_job_name=job, args={})

    qa = QualityAgent()
    suite = load_suite("src/sql/validations/bronze")  # can split per dataset if desired
    # Optionally filter suite to NOAA-only by naming convention
    qa.validate(ctx, database=ATHENA_DB_BRONZE, suite_name="bronze_noaa", validations={
        k:v for k,v in suite.items() if k.startswith("noaa_events_raw")
    })

with DAG(
    dag_id="bronze_ingestion_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["phase5", "bronze"],
) as dag:

    bronze_noaa = PythonOperator(
        task_id="bronze_noaa",
        python_callable=bronze_noaa_task,
        provide_context=True,
    )
