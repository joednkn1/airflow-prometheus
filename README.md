# Airflow Prometheus (0.4.0)

[![PyPI](https://img.shields.io/pypi/v/airflow-prometheus?style=flat-square)](https://pypi.org/project/airflow-prometheus/)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/covid-genomics/airflow-dvc?style=flat-square)](https://github.com/covid-genomics/airflow-prometheus/commits/master)


This is an [Airflow](https://airflow.apache.org/) extension that adds support for generating [Prometheus metrics](https://prometheus.io/).
Package is extension of awesome [airflow-promtheus-exporter project](https://github.com/robinhood/airflow-prometheus-exporter) by [Robinhood](https://github.com/robinhood).

We extended the project, improved the code and added new features to enable better monitoring of your Airflow workloads. :rocket:

<a href="https://covidgenomics.com/">
<img src="https://github.com/covid-genomics/airflow-dvc/blob/master/static/cg_logo.png?raw=true" width="200px"/>
</a>


## Installation

To install this package please do:
```bash
  $ python3 -m pip install "airflow-prometheus==0.4.0"
```

Or if you are using [Poetry](https://python-poetry.org/) to run Apache Airflow:
```bash
  $ poetry add apache-airflow@latest
  $ poetry add "airflow-prometheus@0.4.0"
```

## What this package provides?

* Support for exporting Prometheus metrics
* Support for exporting additional data into Grafana

### Prometheus metrics

Metrics are exported on the `/metrics` endpoint:

| Property                         | Labels                                       | Descriptions                                                                                              |
|----------------------------------|----------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| dag_bag_stats                    | property                                     | Statistics for the dag bag:<br>* `property=loaded_dags_count` - number of loaded DAGs<br>                   |
| airflow_dag_status               | dag_id, owner, status                        | Shows the number of dag starts with this status                                                           |
| airflow_dag_run_duration         | dag_id                                       | Duration of successful dag_runs in seconds                                                                |
| airflow_dag_scheduler_delay      | dag_id                                       | Airflow DAG scheduling delay                                                                              |
| airflow_task_status              | dag_id, task_id, operator_name, owner, state | Shows the number of task instances with particular status                                                 |
| airflow_task_duration            | aggregation, operator_name, task_id, dag_id  | Durations of tasks in seconds by operator:<br>* `aggregation=max`<br>* `aggregation=min`<br>* `aggregation=avg` |
| airflow_task_max_tries           | operator_name, task_id, dag_id               | Max tries for tasks                                                                                       |
| airflow_last_dag_run             | status, task_id, dag_id                      | Tasks status for latest dag run                                                                           |
| airflow_successful_task_duration | task_id, dag_id, execution_date              | Duration of successful tasks in seconds                                                                   |
| airflow_task_fail_count          | dag_id, task_id                              | Count of failed tasks                                                                                     |
| airflow_xcom_parameter           | dag_id, task_id                              | Airflow Xcom Parameter                                                                                    |
| airflow_task_scheduler_delay     | queue                                        | Airflow Task scheduling delay                                                                             |
| airflow_num_queued_tasks         | -                                            | Airflow Number of Queued Tasks                                                                            |


### JSON metadata

You can use [SimpleJson](https://grafana.com/grafana/plugins/grafana-simple-json-datasource/) datasource to display states of DAGs.
Install the plugin with the following command or via grafana.com:
```bash
    $ sudo grafana-cli plugins install grafana-simple-json-datasource
```

Now let's create a json datasource and point it to `/metrics/json/` (trailing slash is important and you may need to check skip TLS verify in order for it to work):

<img src="https://github.com/covid-genomics/airflow-prometheus/blob/master/static/screen1.png?raw=true" width="700px"/>

Now add ad-hoc variable:

<img src="https://github.com/covid-genomics/airflow-prometheus/blob/master/static/screen2.png?raw=true" width="700px"/>

Now you can see ad-hoc filter at the top of the dashboard. You can select DAGs with that filter.
Now we need to add some visualizations.

<img src="https://github.com/covid-genomics/airflow-prometheus/blob/master/static/screen3.png?raw=true" width="700px"/>

We add new panel and select newly created json datasource. As metric we select `dags` and for visualization type: `NodeGraph`


<img src="https://github.com/covid-genomics/airflow-prometheus/blob/master/static/screen4.png?raw=true" width="700px"/>

Node graph will show the dependencies between tasks and their status for the latests instance of the DAG.
DAGs can be selected with the ad-hoc variable you created.
You can remove that ad-hoc filter to show all DAGs, but it's not recommended as NodeGraph panel is fairly bad at zooming or paning the diagram.

  
