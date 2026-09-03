import sys
from pathlib import Path
import pendulum

if not hasattr(pendulum.tz, "timezone") or not callable(pendulum.tz.timezone):
    pendulum.tz.timezone = pendulum.timezone

sys.path.insert(0, str(Path(__file__).parent.parent / "airflow" / "dags"))

from finnhub_dbt_dag import dag


def test_dag_loaded_properly():
    assert dag is not None
    assert dag.dag_id == "finnhub_dbt_run"
    assert dag.catchup is False
    assert len(dag.tasks) == 3


def test_dag_task_dependencies():
    install_task = dag.get_task("install_dbt_dependencies")
    test_task = dag.get_task("test_dbt_connection")
    build_task = dag.get_task("build_dbt_models_and_tests")

    assert test_task in install_task.downstream_list
    assert build_task in test_task.downstream_list
