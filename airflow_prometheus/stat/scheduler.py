"""Prometheus exporter for Airflow."""
from airflow.models import DagRun, TaskInstance
from airflow.settings import Session
from airflow.utils.state import State
from sqlalchemy import and_, func

from .utils import session_scope


def get_dag_scheduler_delay():
    """Compute DAG scheduling delay."""
    with session_scope(Session) as session:
        return (
            session.query(
                DagRun.dag_id, DagRun.execution_date, DagRun.start_date,
            )
            .filter(DagRun._state == State.SUCCESS,)
            .order_by(DagRun.execution_date.desc())
            .limit(1)
            .all()
        )


def get_task_scheduler_delay():
    """Compute Task scheduling delay."""
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.queue,
                func.max(TaskInstance.start_date).label("max_start"),
            )
            .filter(
                TaskInstance.state == State.SUCCESS,
                TaskInstance.queued_dttm.isnot(None),
            )
            .group_by(TaskInstance.queue)
            .subquery()
        )
        return (
            session.query(
                task_status_query.c.queue,
                TaskInstance.queued_dttm,
                task_status_query.c.max_start.label("start_date"),
            )
            .join(
                TaskInstance,
                and_(
                    TaskInstance.queue == task_status_query.c.queue,
                    TaskInstance.start_date == task_status_query.c.max_start,
                ),
            )
            .filter(
                TaskInstance.state == State.SUCCESS,  # Redundant, for performance.
            )
            .all()
        )


def get_num_queued_tasks():
    """Number of queued tasks currently."""
    with session_scope(Session) as session:
        return (
            session.query(TaskInstance)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )

