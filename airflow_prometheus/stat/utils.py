"""Prometheus exporter for Airflow."""
from contextlib import contextmanager

CANARY_DAG = "canary_dag"


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()

