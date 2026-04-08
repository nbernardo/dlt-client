from flask import Blueprint, request
from services.BI.BIService import BIService

bi_controller = Blueprint('bi_controller', __name__)

@bi_controller.route('/analytics/chart/<namespace>', methods=['POST'])
def get_data_catalog(namespace):
    payload = request.get_json()
    return BIService.save_config(
        namespace, payload.get('config'), payload.get('context'), payload.get('title'), payload.get('dataSource')
    )


@bi_controller.route('/analytics/ppline/domains/<namespace>', methods=['GET'])
def get_domain_pipelines(namespace):
    from utils.metastore.PipelineMedatata import PipelineMedatata
    return { 
        'result': {
            'pipelines': PipelineMedatata.get_domain_pipelines(namespace),
            'charts': BIService.get_configs(namespace)
        }, 
        'error': False 
    }


@bi_controller.route('/analytics/ppline/domains/catalog/<namespace>/<pipeline>', methods=['GET'])
def get_domain_pipeline_fields(namespace, pipeline):
    from utils.metastore.DataCatalog import DataCatalog
    return { 'result': DataCatalog.get_fields_by_pipeline(pipeline, namespace), 'error': False }