"""Prometheus exporter for Airflow."""
from prometheus_client.core import GaugeMetricFamily
from airflow_prometheus.stat import get_dag_scheduler_delay


class SchedulerMetricsCollector(object):
    """Metrics Collector for prometheus."""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""
        # Scheduler Metrics
        dag_scheduler_delay = GaugeMetricFamily(
            "airflow_dag_scheduler_delay",
            "Airflow DAG scheduling delay",
            labels=["dag_id"],
        )

        for dag in get_dag_scheduler_delay():
            dag_scheduling_delay_value = (
                    dag.start_date - dag.execution_date
            ).total_seconds()
            dag_scheduler_delay.add_metric(
                [dag.dag_id], dag_scheduling_delay_value
            )
        yield dag_scheduler_delay
