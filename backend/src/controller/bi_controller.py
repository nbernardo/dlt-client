from flask import Blueprint, request
from services.BI.BIService import BIService

bi_controller = Blueprint('bi_controller', __name__)

@bi_controller.route('/analytics/chart/<namespace>', methods=['POST'])
def save_chart(namespace):
    payload = request.get_json()
    return BIService.save_chart(
        namespace, payload.get('config'), payload.get('context'), payload.get('title'), 
        payload.get('dataSource'), payload.get('chartId')
    )


@bi_controller.route('/analytics/dashboard/<namespace>', methods=['POST'])
def save_dashboard(namespace):
    payload = request.get_json()
    return BIService.save_dashboard(namespace, payload.get('charts'), payload.get('name'), payload.get('id'))


from utils.metastore.PipelineMedatata import PipelineMedatata

@bi_controller.route('/analytics/ppline/domains/<namespace>', methods=['GET'])
def get_domain_pipelines(namespace):
    return { 
        'result': {
            'pipelines': PipelineMedatata.get_domain_pipelines(namespace),
            'charts': BIService.get_chart_configs(namespace),
            'dashboards': BIService.get_dashboard_configs(namespace)
        }, 
        'error': False 
    }


@bi_controller.route('/analytics/ppline/dwh/<namespace>', methods=['GET'])
def get_domain_pipeline_list(namespace):
    return PipelineMedatata.get_domain_pipelines(namespace)


from utils.duckdb_util import DuckdbUtil
import platform
from controller.pipeline import BasePipeline

@bi_controller.route('/analytics/ppline/domains/catalog/<namespace>/<pipeline>/<datawarehouse>', methods=['GET'])
def get_domain_pipeline_fields(namespace, pipeline, datawarehouse):
    from utils.metastore.DataCatalog import DataCatalog

    sep = '/' if platform.system() != 'Windows' else '\\\\'
    database_path = f'{BasePipeline.folder}{sep}duckdb{sep}{namespace}{sep}{pipeline}.duckdb'

    table_path = f'{pipeline}.{datawarehouse}'
    range_fields_data = DuckdbUtil.get_range_columns_data(database_path, table_path)
    all_fields = DataCatalog.get_fields_by_pipeline(pipeline, namespace)

    return { 'result': { 'range_fields_data': range_fields_data if range_fields_data != None else [], 'all_fields': all_fields }, 'error': False }



@bi_controller.route('/analytics/integration/odoomodules/<namespace>/<pipeline>', methods=['GET'])
@bi_controller.route('/analytics/integration/odoomodules/<namespace>', methods=['POST'])
def get_odoo_modules(namespace, pipeline = None):
    payload = request.get_json()
    return { 
        'result': {
            'modules': BIService.get_odoo_modules(namespace, payload.get('connectioName'), pipeline),
        }, 
        'error': False 
    }


@bi_controller.route('/analytics/integration/odootables/<module_name>/<namespace>/<pipeline>', methods=['GET'])
@bi_controller.route('/analytics/integration/odootables/<module_name>/<namespace>', methods=['POST'])
def get_odoo_tables(module_name, namespace, pipeline = None):
    payload = request.get_json()
    tables_and_relations = BIService.get_odoo_tables_by_module(module_name, namespace, payload.get('connectioName'), pipeline)
    return { 'result':  tables_and_relations, 'error': False }


@bi_controller.route('/analytics/sql_query/<namespace>', methods=['POST'])
def query_sql_rdbms(namespace):
    payload = request.get_json()
    tables_and_relations = BIService.query_sql_rdbms(payload['query'], namespace, payload['connectionName'])
    return { **tables_and_relations, 'error': False }