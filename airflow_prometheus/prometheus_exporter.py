"""Prometheus exporter for Airflow."""

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_admin import expose
from flask_appbuilder import BaseView as AppBuilderBaseView

from flask import Response
from prometheus_client import generate_latest, REGISTRY
from airflow_prometheus.metrics import TasksMetricsCollector, DagsMetricsCollector,\
    SchedulerMetricsCollector, DagBagMetricsCollector

REGISTRY.register(TasksMetricsCollector())
REGISTRY.register(DagsMetricsCollector())
REGISTRY.register(SchedulerMetricsCollector())
REGISTRY.register(DagBagMetricsCollector())


class Metrics(AppBuilderBaseView):

    @expose("/")
    def index(self):
        return Response(generate_latest(), mimetype="text/plain")

    @expose("/list")
    def list(self):
        return self.render_template(
            "prometheus/list.html",
        )


class AirflowPrometheusPlugin(AirflowPlugin):
    """Airflow Plugin for collecting metrics."""
    name = "airflow_prometheus_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = [
        Blueprint(
            "prometehus_debug_view_bp",
            __name__,
            template_folder="templates",
            static_folder="static",
            static_url_path="/static/prometheus",
        )
    ]
    menu_links = []
    appbuilder_views = [
        {"category": "Admin", "name": "Prometheus metrics", "view": Metrics()}
    ]
    appbuilder_menu_items = []
