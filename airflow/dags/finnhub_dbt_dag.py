from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'finnhub_dbt_run',
    default_args=default_args,
    description='A DAG to trigger daily dbt transformation on ClickHouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'finnhub'],
) as dag:

    # Run dbt deps to make sure any dependencies are installed
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='dbt deps --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    # Run dbt debug to test connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='dbt debug --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    # Build dbt project (runs seeds, models, and tests in DAG order)
    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='dbt build --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    dbt_deps >> dbt_debug >> dbt_build
