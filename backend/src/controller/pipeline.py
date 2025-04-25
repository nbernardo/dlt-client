from flask import Blueprint, request
from services.pipeline.DltPipeline import DltPipeline
from node_mapper.NodeFactory import NodeFactory
from node_mapper.RequestContext import RequestContext
from services.pipeline.DltPipeline import DltPipeline

pipeline = Blueprint('pipeline', __name__)


@pipeline.route('/pipeline/create', methods=['POST'])
def create():
    """
    This is pipeline creation request handler
    """
    payload = request.get_json()
    pipeline_name = payload['activeGrid'] if 'activeGrid' in payload else ''
    context = RequestContext(pipeline_name)

    grid = payload['drawflow'] if 'drawflow' in payload else ''
    start_id = payload['startNode'] if 'startNode' in payload else ''
    node_params = grid['Home']['data']

    start_node = node_params.get(f'{start_id}')
    connections = start_node.get('outputs').get('output_1').values()
    template = DltPipeline.get_template()
    data_place = {}
    parse_node(connections, node_params, data_place, context)

    template = template.replace('%pipeline_name%', f'"{pipeline_name}"')
    for data in data_place.items():
        template = template.replace(data[0], data[1])

    if len(context.exceptions) > 0:
        print(f'TERMINATED WITH EXCEPTIONS: {context.exceptions}')
        return 'Error on creating the pipeline'

    pipeline_instance = DltPipeline()
    pipeline_instance.create_v1(pipeline_name, template)

    return 'From blueprint'


def parse_node(connections, node_params, data_place, context):
    """
    This extract data from everysingle node
    """
    for connection in connections:
        for conn in connection:
            node_id = conn['node']
            node = node_params.get(f'{node_id}')
            node_type = node['name']
            if node_type in ['Start', 'End']:
                pass
            else:
                init_params = node['data']
                inner_cnx = node.get('outputs', {}).get(
                    'output_1', {}).values()
                node = NodeFactory.new_node(node_type, init_params, context)

                for field in node.__dict__.keys():
                    if field not in ['context']:
                        data = node.__dict__[f'{field}']
                        data_place[f'%{field}%'] = f'"{data}"'

                node.run()
                if (len(inner_cnx) > 0):
                    parse_node(inner_cnx, node_params, data_place, context)
