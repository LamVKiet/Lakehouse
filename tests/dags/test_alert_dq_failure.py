"""Unit test for the WAP failure-alert callable (no real SMTP — send_email is patched).
Skips if airflow isn't importable (the DAG module builds operators at import time)."""

import datetime
import os
import sys
from unittest import mock

import pytest

pytest.importorskip("airflow")

# DAG module lives next to the other dags; add the folder to the path so we can import it.
DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "orchestration", "dags")
sys.path.insert(0, os.path.abspath(DAGS_DIR))

import dag_bronze_to_silver as dag_mod  # noqa: E402


def _ctx():
    fake_dag = type("D", (), {"dag_id": "bronze_to_silver_daily"})()
    # ti=None -> failed-task enumeration short-circuits to [] (no DB needed)
    return dict(dag=fake_dag, logical_date=datetime.datetime(2026, 6, 15), task_instance=None)


def test_alert_sends_email_to_oncall():
    with mock.patch.object(dag_mod, "send_email") as send:
        dag_mod.alert_dq_failure(**_ctx())
    send.assert_called_once()
    kwargs = send.call_args.kwargs
    assert kwargs["to"] == dag_mod.ALERT_EMAIL == ["lamkiet345678@gmail.com"]
    # ds = logical_date + 7h offset = 2026-06-15
    assert "2026-06-15" in kwargs["subject"]
    assert "bronze_to_silver_daily" in kwargs["subject"]
    assert "quarantine" in kwargs["html_content"]


def test_smtp_failure_does_not_raise():
    # A misconfigured [smtp] must NOT turn the alert task red (it would mask the real failure).
    with mock.patch.object(dag_mod, "send_email", side_effect=RuntimeError("smtp down")):
        dag_mod.alert_dq_failure(**_ctx())  # swallowed -> no exception
