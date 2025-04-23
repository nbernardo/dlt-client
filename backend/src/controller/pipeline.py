from flask import Blueprint, request
from services.pipeline.DltPipeline import DltPipeline
from node_mapper.NodeFactory import NodeFactory

pipeline = Blueprint('pipeline', __name__)

@pipeline.route('/pipeline/create', methods=['POST'])
def create():
    """
    This is pipeline creation request handler
    """
    
    pipeline = DltPipeline()
    
    payload = request.get_json()
    grid = payload['drawflow']['drawflow'] if 'drawflow' in payload else ''
    node_params = grid['Home']['data'].values()
    
    for node in node_params:
        node_type = node['name']
        init_params = node['data']
        node = NodeFactory.new_node(node_type, init_params)
        node.run()
    
    return 'From blueprint'
