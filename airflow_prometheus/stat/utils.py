"""Prometheus exporter for Airflow."""
from contextlib import contextmanager
from enum import Enum, unique
from typing import Optional


@unique
class ProcessingState(str, Enum):
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


def to_processing_state(raw_value: Optional[str]) -> ProcessingState:
    if raw_value is None:
        raw_value = ProcessingState.NO_STATUS
    else:
        raw_value = str(raw_value)
    assert raw_value in ProcessingState.__members__.values()
    return [getattr(ProcessingState, key) for (key, value) in ProcessingState.__members__.items() if value == raw_value][0]


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()

