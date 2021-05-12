"""
Copyright 2017 Linar <linar@jether-energy.com>
Copyright 2020 Andreas Motl <andreas.motl@panodata.org>
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from flask import Blueprint, request, jsonify, abort
from airflow.models.dagbag import DagBag
import pandas as pd

from airflow.www.app import csrf
from airflow_prometheus.grafana_data.registry import data_generators as dg
from airflow_prometheus.grafana_data.util import dataframe_to_response, dataframe_to_json_table, annotations_to_response

pandas_component = Blueprint('pandas-component', __name__, url_prefix='/metrics/json')
methods = ['GET', 'POST']


@csrf.exempt
@pandas_component.route('/', methods=methods)
def hello_world():
    print(request.headers, request.get_json())
    return 'Jether\'s Grafana Pandas Datasource, used for rendering HTML panels and timeseries data.'


@csrf.exempt
@pandas_component.route('/search', methods=methods)
def find_metrics():
    print(request.headers, request.get_json())
    req = request.get_json()

    if req is None:
        target = '*'
    else:
        target = req.get('target', '*')
    if target == '':
        target = '*'

    if ':' in target:
        finder, target = target.split(':', 1)
    else:
        finder = target

    if not target or finder not in dg.metric_finders:
        metrics = []
        if target == '*':
            #metrics += dg.metric_finders.keys()
            metrics += dg.metric_readers.keys()
            for finder in dg.metric_finders.values():
                metrics += list(finder(target))
        else:
            metrics.append(target)

        metrics = list(set(metrics))
        return jsonify(metrics)
    else:
        return jsonify(list(set(dg.metric_finders[finder](target))))


@csrf.exempt
@pandas_component.route('/query', methods=methods)
def query_metrics():
    print(request.headers, request.get_json())
    req = request.get_json()

    results = []

    ts_range = {'$gt': pd.Timestamp(req['range']['from']).to_pydatetime(),
                '$lte': pd.Timestamp(req['range']['to']).to_pydatetime()}

    if 'intervalMs' in req:
        freq = str(req.get('intervalMs')) + 'ms'
    else:
        freq = None

    for target in req['targets']:
        req_type = target.get('type', 'timeserie')

        target = target['target']
        arg = target
        if ":" in target:
            [target, arg] = target.split(':')
        query_results = dg.metric_readers[target](arg, ts_range)

        if "adhocFilters" in req:
            for adhocFilter in req["adhocFilters"]:
                assert adhocFilter["operator"] == "="
                if isinstance(query_results, list):
                    query_results = [(meta, df.loc[df[adhocFilter["key"]] == adhocFilter["value"]]) for (meta, df) in query_results]
                else:
                    query_results = query_results.loc[query_results[adhocFilter["key"]] == adhocFilter["value"]]

        if req_type == 'table':
            results.extend(dataframe_to_json_table(target, query_results))
        else:
            results.extend(dataframe_to_response(target, query_results, freq=freq))

    return jsonify(results)


@csrf.exempt
@pandas_component.route('/annotations', methods=methods)
def query_annotations():
    print(request.headers, request.get_json())
    req = request.get_json()

    results = []

    ts_range = {'$gt': pd.Timestamp(req['range']['from']).to_pydatetime(),
                '$lte': pd.Timestamp(req['range']['to']).to_pydatetime()}

    query = req['annotation']['query']

    if ':' not in query:
        abort(404, Exception('Target must be of type: <finder>:<metric_query>, got instead: ' + query))

    finder, target = query.split(':', 1)
    results.extend(annotations_to_response(query, dg.annotation_readers[finder](target, ts_range)))

    return jsonify(results)


@csrf.exempt
@pandas_component.route('/panels', methods=methods)
def get_panel():
    print(request.headers, request.get_json())
    req = request.args

    ts_range = {'$gt': pd.Timestamp(int(req['from']), unit='ms').to_pydatetime(),
                '$lte': pd.Timestamp(int(req['to']), unit='ms').to_pydatetime()}

    query = req['query']

    if ':' not in query:
        abort(404, Exception('Target must be of type: <finder>:<metric_query>, got instead: ' + query))

    finder, target = query.split(':', 1)
    return dg.panel_readers[finder](target, ts_range)