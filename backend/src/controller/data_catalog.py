from flask import Blueprint, request
from utils.metastore.meta_storage import MetaStore
from services.modeling.SemanticModel import SemanticModel

datacatalog = Blueprint('datacatalog', __name__)

@datacatalog.route('/datacatalog/<pipeline_name>/catalog/<namespace>', methods=['GET'])
def get_data_catalog(pipeline_name, namespace):
    return MetaStore.get_pipeline_metadata(f'{namespace}_at_{pipeline_name}', None, True)


@datacatalog.route('/datacatalog/<pipeline_name>/<table_name>/catalog/<namespace>', methods=['POST'])
def update_data_catalog(pipeline_name, table_name, namespace):
    rows = request.get_json()
    return SemanticModel.save_semantic_updates(rows, pipeline_name, table_name, namespace)
     