"""Prometheus exporter for Airflow."""
from prometheus_client.core import CounterMetricFamily
from airflow_prometheus.stat import get_dag_bag_info


class DagBagMetricsCollector(object):
    """Metrics Collector for prometheus."""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""
        # Dag Metrics
        dag_bag_info = get_dag_bag_info()
        d_state = CounterMetricFamily(
            "dag_bag_stats",
            "Dag bag stats",
            labels=['property'],
        )
        d_state.add_metric(['loaded_dags_count'], dag_bag_info.loaded_dags_count)

        yield d_state


