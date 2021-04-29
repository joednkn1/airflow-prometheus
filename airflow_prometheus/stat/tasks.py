"""Prometheus exporter for Airflow."""
import json
import pickle

from airflow.configuration import conf
from airflow.models import DagModel, DagRun, TaskInstance, TaskFail, XCom
from airflow.settings import Session
from airflow.utils.state import State
from airflow.utils.log.logging_mixin import LoggingMixin
from sqlalchemy import and_, func
from dataclasses import dataclass
from datetime import datetime

from enum import Enum, unique
from .utils import session_scope
from typing import Generator


@unique
class TaskState(str, Enum):
    SKIPPED = "skipped"
    SUCCESS = "success"
    NO_STATUS = "no_status"
    QUEUED = "queued"
    RUNNING = "running"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    SCHEDULED = "scheduled"
    FAILED = "failed"


@dataclass
class TaskStateInfo:
    task_id: str
    dag_id: str
    operator_name: str
    owner: str
    state: TaskState
    count: float


@dataclass
class TaskFailInfo:
    task_id: str
    dag_id: str
    count: float


@dataclass
class TaskDurationInfo:
    task_id: str
    dag_id: str
    start_date: datetime
    end_date: datetime
    execution_date: datetime

def get_task_state_info() -> Generator[TaskStateInfo, None, None]:
    """Number of task instances with particular state."""
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.operator,
                TaskInstance.state,
                func.count(TaskInstance.dag_id).label("value"),
            )
            .group_by(
                TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state
            )
            .subquery()
        )
        for task in (
            session.query(
                task_status_query.c.dag_id,
                task_status_query.c.task_id,
                task_status_query.c.operator,
                task_status_query.c.state,
                task_status_query.c.value,
                DagModel.owners,
            )
            .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .all()
        ):
            task_state_name = task.state or TaskState.NO_STATUS
            assert task_state_name in TaskState.__members__.values()

            yield TaskStateInfo(
                task_id=task.task_id,
                dag_id=task.dag_id,
                operator_name=task.operator,
                owner=task.owners,
                state=[getattr(TaskState, key) for (key, value) in TaskState.__members__.items() if value == task_state_name][0],
                count=task.value,
            )


def get_task_failure_counts() -> Generator[TaskFailInfo, None, None]:
    """Compute Task Failure Counts."""
    with session_scope(Session) as session:
        for task in (
            session.query(
                TaskFail.dag_id,
                TaskFail.task_id,
                func.count(TaskFail.dag_id).label("count"),
            )
            .join(DagModel, DagModel.dag_id == TaskFail.dag_id,)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .group_by(TaskFail.dag_id, TaskFail.task_id,)
        ):
            yield TaskFailInfo(
                task_id=task.task_id,
                dag_id=task.dag_id,
                count=task.count,
            )


def get_xcom_params(task_id):
    """XCom parameters for matching task_id's for the latest run of a DAG."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        query = session.query(XCom.dag_id, XCom.task_id, XCom.value).join(
            max_execution_dt_query,
            and_(
                (XCom.dag_id == max_execution_dt_query.c.dag_id),
                (
                    XCom.execution_date
                    == max_execution_dt_query.c.max_execution_dt
                ),
            ),
        )
        if task_id == "all":
            return query.all()
        else:
            return query.filter(XCom.task_id == task_id).all()


def extract_xcom_parameter(value):
    """Deserializes value stored in xcom table."""
    enable_pickling = conf.getboolean("core", "enable_xcom_pickling")
    if enable_pickling:
        value = pickle.loads(value)
        try:
            value = json.loads(value)
            return value
        except Exception:
            return {}
    else:
        try:
            return json.loads(value.decode("UTF-8"))
        except ValueError:
            log = LoggingMixin().log
            log.error(
                "Could not deserialize the XCOM value from JSON. "
                "If you are using pickles instead of JSON "
                "for XCOM, then you need to enable pickle "
                "support for XCOM in your airflow config."
            )
            return {}


def get_task_duration_info() -> Generator[TaskDurationInfo, None, None]:
    """Duration of successful tasks in seconds."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id,)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == State.SUCCESS,
                DagRun.end_date.isnot(None),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        for task in (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.start_date,
                TaskInstance.end_date,
                TaskInstance.execution_date,
            )
            .join(
                max_execution_dt_query,
                and_(
                    (TaskInstance.dag_id == max_execution_dt_query.c.dag_id),
                    (
                        TaskInstance.execution_date
                        == max_execution_dt_query.c.max_execution_dt
                    ),
                ),
            )
            .filter(
                TaskInstance.state == State.SUCCESS,
                TaskInstance.start_date.isnot(None),
                TaskInstance.end_date.isnot(None),
            )
            .all()
        ):
            yield TaskDurationInfo(
                task_id=task.task_id,
                dag_id=task.dag_id,
                start_date=task.start_date,
                end_date=task.end_date,
                execution_date=task.execution_date,
            )

