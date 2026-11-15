import logging

log = logging.getLogger(__name__)


def task_failure_alert(context):
    """Called by Airflow automatically when any task fails (after all retries)."""
    ti = context["ti"]
    log.error(
        "ALERT: task '%s' in DAG '%s' failed. Run: %s. Error: %s",
        ti.task_id,
        ti.dag_id,
        context.get("run_id"),
        context.get("exception"),
    )