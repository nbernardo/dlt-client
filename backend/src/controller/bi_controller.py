from flask import Blueprint, request
from services.BI.BIService import BIService
from utils.metastore.BI.PipelinePlan import PipelinePlan

def get_sep(): return '/' if platform.system() != 'Windows' else '\\\\'

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


@bi_controller.route('/analytics/ppline/dwh/staged', methods=['GET'])
def get_staged_data(namespace):
    return PipelineMedatata.get_stage_data(namespace)


from utils.duckdb_util import DuckdbUtil
import platform
from controller.pipeline import BasePipeline

@bi_controller.route('/analytics/ppline/domains/catalog/<namespace>/<pipeline>/<datawarehouse>', methods=['GET'])
def get_domain_pipeline_fields(namespace, pipeline, datawarehouse):
    from utils.pipeline.PipelinesHelper import get_table_columns
    from utils.metastore.PipelineMedatata import PipelineMedatata

    sep = get_sep()
    database_path = f'{BasePipeline.folder}{sep}duckdb{sep}{namespace}{sep}{pipeline}.duckdb'

    table_path = f'{pipeline}.{datawarehouse}'
    range_fields_data = DuckdbUtil.get_range_columns_data(database_path, table_path)
    metadatas = PipelineMedatata.get_pipeline_metadata(pipeline, namespace)
    #all_fields = DataCatalog.get_fields_by_pipeline(pipeline, namespace)
    all_fields = get_table_columns(database_path, pipeline)

    return { 
        'result': { 
            'range_fields_data': range_fields_data if range_fields_data != None else [], 
            'all_fields': all_fields, 
            'secret_name': list(metadatas)[2] if len(list(metadatas)) > 3 else None
        }, 
        'error': False 
    }



@bi_controller.route('/analytics/integration/odoomodules/<namespace>/<pipeline>', methods=['GET'])
@bi_controller.route('/analytics/integration/odoomodules/<namespace>', methods=['POST'])
def get_odoo_modules(namespace, pipeline = None, dataset_name = None):
    # duckDBFile = pipeline, hence the same param is used for both scenario 
    payload = request.get_json()
    result = BIService.get_db_tables(namespace, payload.get('connectioName'), pipeline)
    return { 
        'result': { 
            'tables': result.get('result', []), 'db_name': result.get('db_name'), 
            'db_host': result.get('db_host'), 'db_engine': result.get('db_engine')
        },
        'error': False 
    }


@bi_controller.route('/analytics/integration/odootables/<anchor_table>/<namespace>/<pipeline>', methods=['GET'])
@bi_controller.route('/analytics/integration/odootables/<anchor_table>/<namespace>', methods=['POST'])
def get_odoo_tables(anchor_table, namespace, pipeline = None):
    payload = request.get_json()
    tables_and_relations = BIService.get_odoo_tables_hierarchy(anchor_table, namespace, payload.get('connectioName'), pipeline)
    return { 'result':  tables_and_relations, 'error': False }


@bi_controller.route('/analytics/sql_query/<namespace>', methods=['POST'])
def query_sql_rdbms(namespace):
    payload = request.get_json()
    return BIService.query_sql_rdbms(payload.get('query'), namespace, payload.get('connectionName'))


@bi_controller.route('/analytics/<namespace>/pipeline/plan', methods=['POST'])
def create_pipeline_plan(namespace):
    payload = request.get_json()
    result = PipelinePlan.create_new_plan(namespace, payload.get('settings'), payload.get('update'), payload.get('id'))
    return result


@bi_controller.route('/analytics/<namespace>/pipeline/plan', methods=['GET'])
@bi_controller.route('/analytics/<namespace>/pipeline/plan/<id>', methods=['GET'])
def get_pipeline_plan(namespace, id = None):
    result = PipelinePlan.get_plans(namespace, id)
    return { 'error': False, 'result': result }


@bi_controller.route('/pipeline/dictionary/<namespace>/<pipeline>', methods=['POST'])
def upsert_dictionary(namespace, pipeline):

    from utils.pipeline.DataDictionary import DataDictionary
    payload = request.get_json()
    pipeline = pipeline.split('.')[0] if pipeline.__contains__('.') else pipeline
    
    sep = get_sep()
    database_path = f'{BasePipeline.folder}{sep}duckdb{sep}{namespace}{sep}{pipeline}.duckdb'

    return DataDictionary.upsert_dictionary(database_path, payload.get('values',[]))


from services.modeling.dw.DeclarationModeling import DeclarationModeling

@bi_controller.route('/declaration/model/<namespace>', methods=['POST'])
def persiste_model(namespace):
    payload = request.get_json()
    declaration, modelQuery, quality = payload.get('model'), payload.get('modelQuery'), payload.get('quality')
    dw = payload.get('dw','').split('.')
    dw, model_name = '.'.join(dw[-2:3]), payload.get('modelName')
    if(quality):
        return DeclarationModeling().persist_quality_rules(namespace, dw, declaration, modelQuery, model_name)
    return DeclarationModeling().persist_model(namespace, dw, declaration, modelQuery, model_name)
