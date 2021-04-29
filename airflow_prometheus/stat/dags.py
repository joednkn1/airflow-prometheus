"""Prometheus exporter for Airflow."""
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.settings import Session
from airflow.utils.state import State
from sqlalchemy import and_, func
from dataclasses import dataclass

from typing import Generator
from .utils import session_scope, ProcessingState, to_processing_state
from datetime import datetime


@dataclass
class DagStateInfo:
    dag_id: str
    owner: str
    state: ProcessingState
    count: float


@dataclass
class DagDurationInfo:
    dag_id: str
    start_date: datetime
    end_date: datetime


def get_dag_state_info() -> Generator[DagStateInfo, None, None]:
    """Number of DAG Runs with particular state."""
    with session_scope(Session) as session:
        dag_status_query = (
            session.query(
                DagRun.dag_id,
                DagRun.state,
                func.count(DagRun.state).label("count"),
            )
            .group_by(DagRun.dag_id, DagRun.state)
            .subquery()
        )
        for dag in (
            session.query(
                dag_status_query.c.dag_id,
                dag_status_query.c.state,
                dag_status_query.c.count,
                DagModel.owners,
            )
            .join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .all()
        ):
            yield DagStateInfo(
                dag_id=dag.dag_id,
                owner=dag.owners,
                count=dag.count,
                state=to_processing_state(dag.state),
            )


def get_dag_duration_info() -> Generator[DagDurationInfo, None, None]:
    """Duration of successful DAG Runs."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == State.SUCCESS,
                DagRun.end_date.isnot(None),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        dag_start_dt_query = (
            session.query(
                max_execution_dt_query.c.dag_id,
                max_execution_dt_query.c.max_execution_dt.label(
                    "execution_date"
                ),
                func.min(TaskInstance.start_date).label("start_date"),
            )
            .join(
                TaskInstance,
                and_(
                    TaskInstance.dag_id == max_execution_dt_query.c.dag_id,
                    (
                        TaskInstance.execution_date
                        == max_execution_dt_query.c.max_execution_dt
                    ),
                ),
            )
            .group_by(
                max_execution_dt_query.c.dag_id,
                max_execution_dt_query.c.max_execution_dt,
            )
            .subquery()
        )

        for dag in (
            session.query(
                dag_start_dt_query.c.dag_id,
                dag_start_dt_query.c.start_date,
                DagRun.end_date,
            )
            .join(
                DagRun,
                and_(
                    DagRun.dag_id == dag_start_dt_query.c.dag_id,
                    DagRun.execution_date
                    == dag_start_dt_query.c.execution_date,
                ),
            )
            .filter(
                TaskInstance.start_date.isnot(None),
                TaskInstance.end_date.isnot(None),
            )
            .all()
        ):
            yield DagDurationInfo(
                dag_id=dag.dag_id,
                start_date=dag.start_date,
                end_date=dag.end_date,
            )


