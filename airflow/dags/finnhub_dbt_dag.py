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

    # Run dbt seed to load seeds into ClickHouse
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='dbt seed --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    # Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    # Test dbt models
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
    )

    dbt_deps >> dbt_debug >> dbt_seed >> dbt_run >> dbt_test
