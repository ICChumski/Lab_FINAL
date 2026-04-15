from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "ivan",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="mobility_pipeline",
    default_args=default_args,
    description="Pipeline ELT do projeto final de mobilidade",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["mobility", "elt", "final_project"],
) as dag:

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/04_load_raw_postgres.py"
        ),
    )

    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_ge_checkpoint.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt/mobility_dbt && "
            "dbt run"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt/mobility_dbt && "
            "dbt test"
        ),
    )

    load_raw >> validate_raw >> dbt_run >> dbt_test