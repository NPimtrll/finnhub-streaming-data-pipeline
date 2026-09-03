from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='finnhub_dbt_run',
    default_args=default_args,
    description='A DAG to trigger daily dbt transformation on ClickHouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'finnhub'],
) as dag:

    install_dbt_dependencies = BashOperator(
        task_id='install_dbt_dependencies',
        bash_command=f'dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}',
    )

    test_dbt_connection = BashOperator(
        task_id='test_dbt_connection',
        bash_command=f'dbt debug --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}',
    )

    build_dbt_models_and_tests = BashOperator(
        task_id='build_dbt_models_and_tests',
        bash_command=f'dbt build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}',
    )

    install_dbt_dependencies >> test_dbt_connection >> build_dbt_models_and_tests
