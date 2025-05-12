from flask import Blueprint, request
from services.pipeline.DltPipeline import DltPipeline
from node_mapper.NodeFactory import NodeFactory
from .RequestContext import RequestContext
from node_mapper.TemplateNodeType import TemplateNodeType

escape_component_field = ['context', 'component_id','template']
pipeline = Blueprint('pipeline', __name__)


@pipeline.route('/pipeline/create', methods=['POST'])
def create():
    """
    This is pipeline creation request handler
    """
    payload = request.get_json()
    pipeline_name = payload['activeGrid'] if 'activeGrid' in payload else ''
    context = RequestContext(pipeline_name, payload['socketSid'])

    grid = payload['drawflow'] if 'drawflow' in payload else ''
    start_id = payload['startNode'] if 'startNode' in payload else ''
    node_params = grid['Home']['data']

    start_node = node_params.get(f'{start_id}')
    connections = start_node.get('outputs').get('output_1').values()
    first_connection_id = list(connections)[0][0]['node']
    fst_connection = node_params.get(first_connection_id)
    template = NodeFactory.new_node(fst_connection.get('name',None)).template
    data_place, node_list = {}, []
    all_nodes: list[TemplateNodeType] = None

    all_nodes = parse_node(connections, node_params, data_place, context, node_list)

    template = template.replace('%pipeline_name%', f'"{pipeline_name}"')        

    for data in data_place.items():
        value = data[1] if check_type(data[1]) else str(data[1])
        template = template.replace(data[0], value)

    if len(context.exceptions) > 0:
        print(f'TERMINATED WITH EXCEPTIONS: {context.exceptions}')
        return 'Error on creating the pipeline'

    pipeline_instance = DltPipeline()
    result = pipeline_instance.create_v1(pipeline_name, template, context)
    if result['status'] is False:
        for node in all_nodes:
            node.notify_failure_to_ui('Pipeline',result['message'],False)

    return result['message']


def parse_node(connections, node_params, data_place, context, node_list: list):
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
                node_list.append(node)

                for field in node.__dict__.keys():
                    if field not in escape_component_field:
                        data = node.__dict__[f'{field}']
                        data_place[f'%{field}%'] = f'"{data}"' if type(data) == str else data

                if node.run() is RequestContext.FAILED:
                    break

                if (len(inner_cnx) > 0):
                    parse_node(inner_cnx, node_params, data_place, context, node_list)

    return node_list


def check_type(val):
    tp = type(val)
    return tp == int\
            or tp == float\
            or tp == str\
            or tp == bool