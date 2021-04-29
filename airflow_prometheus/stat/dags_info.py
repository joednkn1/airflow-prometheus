from airflow.models.dagbag import DagBag
from dataclasses import dataclass
from typing import Dict
from collections import defaultdict


@dataclass
class DagBagInfo:
    loaded_dags_count: int
    tasks: Dict[str, int]


def get_dag_bag_info() -> DagBagInfo:
    loaded_dags_count = 0
    tasks: Dict[str, int] = defaultdict(int)
    for dag in DagBag().dags.values():
        loaded_dags_count += 1
        for task in dag.tasks:
            tasks[task.__class__.__name__] += 1

    return DagBagInfo(
        loaded_dags_count=loaded_dags_count,
        tasks=tasks,
    )
