# noqa: D100


from datetime import datetime

from airflow.sdk import dag, task

from de_projet_perso.example import test_import_in_dags, test_with_custom_logger


@task.bash
def echo_working():  # noqa: D103
    return 'echo "working !"'


@task.bash
def echo_only_if_first_succeed():  # noqa: D103
    return 'echo "only_if_first_succeed !"'


@task.python
def echo_func_from_package():  # noqa: D103
    print(test_import_in_dags(x=10))


@task.python
def echo_func_from_package_with_custom_logger():  # noqa: D103
    test_with_custom_logger()


@dag(
    dag_id="example_working",
    description="Un DAG simple pour tester que tout fonctionne",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def example_dag():  # noqa: D103
    echo_working() >> echo_only_if_first_succeed()
    echo_func_from_package() >> echo_func_from_package_with_custom_logger()


example_dag()
