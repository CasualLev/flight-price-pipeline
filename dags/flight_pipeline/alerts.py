import logging
import os
import smtplib
from email.message import EmailMessage

log = logging.getLogger(__name__)


def send_alert_email(subject, body):
    """Send an email alert. Never crashes: if email fails, it logs the problem."""
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASSWORD")
    to = os.getenv("ALERT_EMAIL")

    if not (user and password and to):
        log.warning("Email alert skipped: SMTP settings are missing")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    msg.set_content(body)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(user, password)
            server.send_message(msg)
        log.info("Alert email sent to %s", to)
    except Exception as e:
        log.error("Could not send alert email: %s", e)


def task_failure_alert(context):
    """Called by Airflow automatically when any task fails (after all retries)."""
    ti = context["ti"]
    error = context.get("exception")

    log.error(
        "ALERT: task '%s' in DAG '%s' failed. Run: %s. Error: %s",
        ti.task_id, ti.dag_id, context.get("run_id"), error,
    )

    send_alert_email(
        subject=f"[Airflow] Task failed: {ti.dag_id}.{ti.task_id}",
        body=(
            f"DAG: {ti.dag_id}\n"
            f"Task: {ti.task_id}\n"
            f"Run: {context.get('run_id')}\n"
            f"Error: {error}\n\n"
            "Check the task logs in the Airflow UI for details."
        ),
    )