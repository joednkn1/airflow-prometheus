"""Prometheus exporter for Airflow."""
import json
import pickle

from airflow.configuration import conf
from airflow.models import DagModel, DagRun, TaskInstance, TaskFail, XCom
from airflow.settings import Session
from airflow.utils.state import State
from airflow.models.dagbag import DagBag
from airflow.utils.log.logging_mixin import LoggingMixin
from sqlalchemy import and_, func
from sqlalchemy.exc import InvalidRequestError
from dataclasses import dataclass
from datetime import datetime

from .utils import session_scope, ProcessingState, to_processing_state
from typing import Generator, Dict, List

def check_if_can_query_tasks():
    try:
        with session_scope(Session) as session:
            return len(session.query(TaskInstance.task_id).all()) > 0
    except:
        return False

@dataclass
class TaskStateInfo:
    task_id: str
    dag_id: str
    operator_name: str
    owner: str
    state: ProcessingState
    count: float
    avg_duration: float
    min_duration: float
    max_duration: float
    max_tries: int


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


@dataclass
class LatestTaskInfo:
    task_id: str
    dag_id: str
    execution_date: datetime
    state: ProcessingState
    duration: float


def get_latest_tasks_state_info_for_all_dags() -> List[LatestTaskInfo]:
    if not check_if_can_query_tasks():
        return []
    results: List[LatestTaskInfo] = []
    for dag in DagBag().dags.values():
        meta = get_latest_tasks_state_info(dag.dag_id)
        results += list(meta.values())
    return results


def get_latest_tasks_state_info(dag_id: str) -> Dict[str, LatestTaskInfo]:
    if not check_if_can_query_tasks():
        return dict()
    with session_scope(Session) as session:
        latest_dag_execution_date = (
            session.query(
                DagRun.execution_date,
                DagRun.dag_id,
            )
            .filter(DagRun.dag_id == dag_id)
            .order_by(DagRun.execution_date.desc())
            .limit(1)
            .all()
        )
        if len(latest_dag_execution_date) == 0:
            return dict()

        latest_dag_execution_date = latest_dag_execution_date[0].execution_date
        latest_task_runs = (
            session.query(
                TaskInstance.state,
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.duration,
                DagRun.execution_date
            )
            .join(DagRun, DagRun.dag_id == TaskInstance.dag_id)
            .filter(DagRun.execution_date == latest_dag_execution_date)
            .all()
        )
        latest_task_runs_dict = dict()
        for latest_task_run in latest_task_runs:
            latest_task_runs_dict[latest_task_run.task_id] = LatestTaskInfo(
                task_id=latest_task_run.task_id,
                dag_id=latest_task_run.dag_id,
                execution_date=latest_task_run.execution_date,
                state=to_processing_state(latest_task_run.state),
                duration=latest_task_run.duration or 0,
            )
        return latest_task_runs_dict


def get_task_state_info() -> Generator[TaskStateInfo, None, None]:
    """Number of task instances with particular state."""
    if not check_if_can_query_tasks():
        return
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.operator,
                TaskInstance.state,
                func.count(TaskInstance.dag_id).label("value"),
                func.avg(TaskInstance.duration).label("avg_duration"),
                func.min(TaskInstance.duration).label("min_duration"),
                func.max(TaskInstance.duration).label("max_duration"),
                func.max(TaskInstance.max_tries).label("max_tries"),
            )
            .group_by(
                TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state, TaskInstance.operator,
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
                task_status_query.c.avg_duration,
                task_status_query.c.min_duration,
                task_status_query.c.max_duration,
                task_status_query.c.max_tries,
                DagModel.owners,
            )
            .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
            )
            .all()
        ):
            yield TaskStateInfo(
                task_id=task.task_id,
                dag_id=task.dag_id,
                operator_name=task.operator,
                owner=task.owners,
                state=to_processing_state(task.state),
                count=task.value,
                avg_duration=task.avg_duration or 0,
                min_duration=task.min_duration or 0,
                max_duration=task.max_duration or 0,
                max_tries=task.max_tries,
            )


def get_task_failure_counts() -> Generator[TaskFailInfo, None, None]:
    """Compute Task Failure Counts."""
    if not check_if_can_query_tasks():
        return
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
    if not check_if_can_query_tasks():
        return []
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
    if not check_if_can_query_tasks():
        return
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
                DagRun.execution_date
            )
            .join(DagRun, DagRun.dag_id == TaskInstance.dag_id)
            .join(
                max_execution_dt_query,
                and_(
                    (TaskInstance.dag_id == max_execution_dt_query.c.dag_id),
                    (
                        DagRun.execution_date
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

