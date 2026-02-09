from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from src.agents.base import AgentContext
from src.agents.transform_agent import TransformAgent
from src.agents.quality_agent import QualityAgent
from src.ops.validators import load_suite
from src.ops.config import ATHENA_DB_SILVER

DEFAULT_ARGS = {"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)}

def silver_noaa_task(**context):
    run_id = context["run_id"]
    ctx = AgentContext(run_id=run_id, dag="silver_transform_dag", dataset="noaa", layer="silver")

    job = Variable.get("GLUE_JOB_NOAA_SILVER")
    tf = TransformAgent()
    tf.trigger_silver_job(ctx, glue_job_name=job, args={})

    qa = QualityAgent()
    suite = load_suite("src/sql/validations/silver")
    qa.validate(ctx, database=ATHENA_DB_SILVER, suite_name="silver_noaa", validations={
        k:v for k,v in suite.items() if k.startswith("noaa_events_clean")
    })

with DAG(
    dag_id="silver_transform_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["phase5", "silver"],
) as dag:
    silver_noaa = PythonOperator(task_id="silver_noaa", python_callable=silver_noaa_task, provide_context=True)
