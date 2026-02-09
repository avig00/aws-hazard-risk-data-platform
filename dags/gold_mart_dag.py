from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.agents.base import AgentContext
from src.agents.gold_mart_agent import GoldMartAgent
from src.agents.quality_agent import QualityAgent
from src.agents.catalog_agent import CatalogAgent
from src.ops.validators import load_suite
from src.ops.config import ATHENA_DB_GOLD

def read_sql(path: str) -> str:
    from pathlib import Path
    return Path(path).read_text(encoding="utf-8")

DEFAULT_ARGS = {"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=5)}

def build_gold(**context):
    run_id = context["run_id"]
    ctx = AgentContext(run_id=run_id, dag="gold_mart_dag", layer="gold")

    gold = GoldMartAgent()
    qa = QualityAgent()
    cat = CatalogAgent()

    # Build hazard_event_summary
    sql1 = read_sql("src/sql/gold_ctas/build_hazard_event_summary.sql")
    gold.run_ctas(ctx, database=ATHENA_DB_GOLD, sql=sql1, name="hazard_event_summary")
    suite_gold = load_suite("src/sql/validations/gold")
    qa.validate(ctx, database=ATHENA_DB_GOLD, suite_name="gold_hazard_event_summary", validations={
        k:v for k,v in suite_gold.items() if k.startswith("hazard_event_summary")
    })

    # Build risk_feature_mart
    sql2 = read_sql("src/sql/gold_ctas/build_risk_feature_mart.sql")
    gold.run_ctas(ctx, database=ATHENA_DB_GOLD, sql=sql2, name="risk_feature_mart")
    qa.validate(ctx, database=ATHENA_DB_GOLD, suite_name="gold_risk_feature_mart", validations={
        k:v for k,v in suite_gold.items() if k.startswith("risk_feature_mart")
    })

    # Catalog integrity: table exists + repair partitions if applicable
    for t in ["hazard_event_summary", "risk_feature_mart"]:
        exists = cat.ensure_table_exists(ctx, database=ATHENA_DB_GOLD, table=t)
        if not exists:
            raise RuntimeError(f"Glue table missing: {ATHENA_DB_GOLD}.{t}")

with DAG(
    dag_id="gold_mart_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["phase5", "gold"],
) as dag:
    build_gold_task = PythonOperator(task_id="build_gold", python_callable=build_gold, provide_context=True)
