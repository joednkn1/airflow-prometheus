from airflow_prometheus.grafana_data.registry import data_generators as dg
from airflow_prometheus.grafana_data.service import pandas_component, methods
from airflow.models.dagbag import DagBag
import pandas as pd
from flask import request, jsonify
from airflow.www.app import csrf
from airflow_prometheus.stat import get_latest_tasks_state_info, LatestTaskInfo, ProcessingState
from typing import Dict


@csrf.exempt
@pandas_component.route('/tag-keys', methods=methods)
def tag_keys():
    return jsonify([
        dict(type="string", text="dag_id"),
    ])


@csrf.exempt
@pandas_component.route('/tag-values', methods=methods)
def tag_values():
    req = request.get_json()
    if req is None or "key" not in req:
        key = ""
    else:
        key = req["key"]
    data = []
    if key == "dag_id":
        data = [dag.dag_id for dag in DagBag().dags.values()]
    return jsonify([dict(text=item) for item in data])


def get_dags_metrics(_):
    return ["dags", *[f"dags:{dag.dag_id}" for dag in DagBag().dags.values()]]


def get_dags(_, ts_range):
    free_id = 0
    nodes = dict()
    edges = dict()
    ids_mapping = dict()

    for dag in DagBag().dags.values():
        for task in dag.tasks:
            if task.task_id not in ids_mapping:
                ids_mapping[task.task_id] = free_id
                free_id += 1
            # task.upstream_task_ids

    latest_tasks_info: Dict[str, Dict[str, LatestTaskInfo]] = dict()

    for dag in DagBag().dags.values():
        if dag.dag_id not in latest_tasks_info:
            latest_tasks_info[dag.dag_id] = get_latest_tasks_state_info(dag.dag_id)
        for task in dag.tasks:
            node_id = ids_mapping[task.task_id]
            prev = dict()
            if node_id in nodes:
                prev = nodes[node_id]
            nodes[node_id] = {
                **prev,
                **dict(
                    id=node_id,
                    task_id=task.task_id,
                    dag_id=dag.dag_id,
                    title=task.task_id,
                    subTitle=task.__class__.__name__,
                    mainStat=0,
                    secondaryStat=0),
            }
            for subnode in task.downstream_list:
                subnode_id = ids_mapping[subnode.task_id]
                if subnode_id not in nodes:
                    nodes[subnode_id] = dict()
                if node_id != subnode_id:
                    edges[f"{node_id}--{subnode_id}"] = dict(
                        id=f"{node_id}--{subnode_id}",
                        source=node_id,
                        target=subnode_id,
                        dag_id=dag.dag_id,
                        task_id=task.task_id,
                    )

    for node_id in nodes.keys():
        node = nodes[node_id]
        latest_task_meta_dict = latest_tasks_info[node["dag_id"]]
        if node["task_id"] in latest_task_meta_dict:
            meta = latest_task_meta_dict[node["task_id"]]
            node["mainStat"] = meta.state
            node["secondaryStat"] = f"{meta.duration} sec"
        else:
            node["mainStat"] = ProcessingState.NO_STATUS
            node["secondaryStat"] = ""

        status = node["mainStat"]
        if status == ProcessingState.SUCCESS:
            node["node_graph_green"] = 1
            node["node_graph_red"] = 0
            node["node_graph_yellow"] = 0
            node["node_graph_gray"] = 0
            node["node_graph_purple"] = 0
        elif status == ProcessingState.FAILED:
            node["node_graph_green"] = 0
            node["node_graph_red"] = 1
            node["node_graph_yellow"] = 0
            node["node_graph_gray"] = 0
            node["node_graph_purple"] = 0
        elif status == ProcessingState.RUNNING:
            node["node_graph_green"] = 0
            node["node_graph_red"] = 0
            node["node_graph_yellow"] = 0
            node["node_graph_gray"] = 0
            node["node_graph_purple"] = 1
        elif status in [ProcessingState.QUEUED, ProcessingState.SCHEDULED]:
            node["node_graph_green"] = 0
            node["node_graph_red"] = 0
            node["node_graph_yellow"] = 0
            node["node_graph_gray"] = 1
            node["node_graph_purple"] = 0
        else:
            node["node_graph_green"] = 0
            node["node_graph_red"] = 0
            node["node_graph_yellow"] = 0
            node["node_graph_gray"] = 1
            node["node_graph_purple"] = 0

        nodes[node_id] = node

    nodes = list(nodes.values())
    edges = list(edges.values())

    nodes_df, edges_df = pd.DataFrame(nodes), pd.DataFrame(edges)
    return [
        (dict(name='nodes', meta=dict(preferredVisualisationType='nodeGraph')), nodes_df),
        (dict(name='edges', meta=dict(preferredVisualisationType='nodeGraph')), edges_df),
    ]


def init_json_exporters():
    # Register data generators.
    dg.add_metric_reader("dags", get_dags)

