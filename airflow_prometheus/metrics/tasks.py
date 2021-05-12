"""Prometheus exporter for Airflow."""
from prometheus_client.core import GaugeMetricFamily
from airflow_prometheus.stat import get_task_state_info, extract_xcom_parameter, get_xcom_params,\
    get_num_queued_tasks, get_task_duration_info, get_task_failure_counts, \
    get_task_scheduler_delay, get_latest_tasks_state_info_for_all_dags
from airflow_prometheus.xcom_config import load_xcom_config


class TasksMetricsCollector(object):
    """Metrics Collector for prometheus."""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""
        # Task metrics
        task_info = get_task_state_info()

        t_state = GaugeMetricFamily(
            "airflow_task_status",
            "Shows the number of task instances with particular status",
            labels=["dag_id", "task_id", "operator_name", "owner", "state"],
        )
        for task in task_info:
            t_state.add_metric(
                [task.dag_id, task.task_id, task.operator_name, task.owner, task.state],
                task.count,
            )
        yield t_state

        task_detailed_durations = GaugeMetricFamily(
            "airflow_task_duration",
            "Durations of tasks in seconds by operator",
            labels=["aggregation", "operator_name", "task_id", "dag_id"],
        )
        for task in task_info:
            task_detailed_durations.add_metric(
                ["avg", task.operator_name, task.task_id, task.dag_id],
                task.avg_duration,
            )
            task_detailed_durations.add_metric(
                ["min", task.operator_name, task.task_id, task.dag_id],
                task.min_duration,
            )
            task_detailed_durations.add_metric(
                ["max", task.operator_name, task.task_id, task.dag_id],
                task.max_duration,
            )
        yield task_detailed_durations

        task_max_tries = GaugeMetricFamily(
            "airflow_task_max_tries",
            "Max tries for tasks",
            labels=["operator_name", "task_id", "dag_id"],
        )
        for task in task_info:
            task_detailed_durations.add_metric(
                [task.operator_name, task.task_id, task.dag_id],
                task.max_tries,
            )
        yield task_max_tries

        last_dag_run = GaugeMetricFamily(
            "airflow_last_dag_run",
            "Tasks status for latest dag run",
            labels=["status", "task_id", "dag_id"],
        )
        for task in get_latest_tasks_state_info_for_all_dags():
            last_dag_run.add_metric(
                [task.state, task.task_id, task.dag_id],
                task.duration,
            )
        yield last_dag_run

        successful_task_duration = GaugeMetricFamily(
            "airflow_successful_task_duration",
            "Duration of successful tasks in seconds",
            labels=["task_id", "dag_id", "execution_date"],
        )
        for task in get_task_duration_info():
            task_duration_value = (
                task.end_date - task.start_date
            ).total_seconds()
            successful_task_duration.add_metric(
                [task.task_id, task.dag_id, str(task.execution_date.date())],
                task_duration_value,
            )
        yield successful_task_duration

        task_failure_count = GaugeMetricFamily(
            "airflow_task_fail_count",
            "Count of failed tasks",
            labels=["dag_id", "task_id"],
        )
        for task in get_task_failure_counts():
            task_failure_count.add_metric(
                [task.dag_id, task.task_id], task.count
            )
        yield task_failure_count

        xcom_params = GaugeMetricFamily(
            "airflow_xcom_parameter",
            "Airflow Xcom Parameter",
            labels=["dag_id", "task_id"],
        )

        xcom_config = load_xcom_config()
        for tasks in xcom_config.get("xcom_params", []):
            for param in get_xcom_params(tasks["task_id"]):
                xcom_value = extract_xcom_parameter(param.value)

                if tasks["key"] in xcom_value:
                    xcom_params.add_metric(
                        [param.dag_id, param.task_id], xcom_value[tasks["key"]]
                    )

        yield xcom_params

        task_scheduler_delay = GaugeMetricFamily(
            "airflow_task_scheduler_delay",
            "Airflow Task scheduling delay",
            labels=["queue"],
        )

        for task in get_task_scheduler_delay():
            task_scheduling_delay_value = (
                    task.start_date - task.queued_dttm
            ).total_seconds()
            task_scheduler_delay.add_metric(
                [task.queue], task_scheduling_delay_value
            )
        yield task_scheduler_delay

        num_queued_tasks_metric = GaugeMetricFamily(
            "airflow_num_queued_tasks", "Airflow Number of Queued Tasks",
        )

        num_queued_tasks = get_num_queued_tasks()
        num_queued_tasks_metric.add_metric([], num_queued_tasks)
        yield num_queued_tasks_metric