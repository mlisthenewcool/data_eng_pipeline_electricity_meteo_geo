"""TODO docstring."""

from datetime import datetime

from airflow.sdk import DAG, dag, task


def create_error_dag(dag_id: str, error_message: str) -> DAG:
    """Create a placeholder DAG that surfaces the error in Airflow UI.

    This prevents Airflow scheduler from crashing while making the error
    visible to operators via the UI.

    Args:
        dag_id: Unique DAG identifier (includes dataset name for uniqueness)
        error_message: Error to display
    """
    error_docs = f"""
    # ❌ ERREUR DE CONFIGURATION
    **DAG ID:** `{dag_id}`

    ### Message d'erreur :
    > {error_message}

    ---
    **Action requise**
    """

    @dag(
        dag_id=dag_id,
        description=f" ❌ FAILED: {error_message}",
        schedule=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["error", "needs-attention"],
        doc_md=error_docs,
    )
    def error_dag() -> None:
        @task(task_id="CLIQUEZ_ICI_POUR_VOIR_L_ERREUR")
        def report_error() -> None:
            """Surfaces the catalog/DAG creation error."""
            raise RuntimeError(error_message)

        report_error()

    return error_dag()
