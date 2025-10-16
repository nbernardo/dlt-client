from flask import Blueprint, request, jsonify
from services.pipeline.DltPipeline import DltPipeline
from node_mapper.NodeFactory import NodeFactory
from .RequestContext import RequestContext
from node_mapper.TemplateNodeType import TemplateNodeType
import os
import pandas as pd

escape_component_field = ['context', 'component_id','template']
pipeline = Blueprint('pipeline', __name__)

class BasePipeline:
    folder = None


@pipeline.route('/pipeline/create', methods=['POST','PUT'])
def create():
    """ This is pipeline creation request handler """
    
    payload = request.get_json()
    
    pipeline_name, pipeline_lbl = set_pipeline_name(payload)
    context = RequestContext(pipeline_name, payload['socketSid'])
    context.action_type = 'UPDATE' if request.method == 'PUT' else None

    duckdb_path, ppline_path, diagrm_path = handle_user_tenancy_folders(payload, context)
    start_node_id, node_params = pepeline_init_param(payload)
    node_params = parse_transformation_task(node_params, context)

    start_node = node_params.get(f'{start_node_id}')
    connections = start_node.get('outputs').get('output_1').values()
    first_connection_id = list(connections)[0][0]['node']
    fst_connection = node_params.get(first_connection_id)

    is_cloud_bucket_req = False
    if 'bucketFileSource' in fst_connection['data']:
        is_cloud_bucket_req = int(fst_connection['data']['bucketFileSource']) == 2

    context.ppline_path = ppline_path
    context.diagrm_path = diagrm_path
    context.pipeline_lbl = pipeline_lbl
    context.connections = connections
    context.node_params = node_params
    context.is_cloud_url = True if is_cloud_bucket_req else False

    if(context.action_type == 'UPDATE'):
        result =  create_new_version_ppline(fst_connection, 
                        pipeline_name, 
                        payload, 
                        duckdb_path, 
                        context)
        
        if not context.success_emitted:
            context.emit_ppsuccess()
        return result
    else:
        return create_new_ppline(fst_connection, 
                        pipeline_name, 
                        payload, 
                        duckdb_path, 
                        context)



def create_new_ppline(fst_connection, 
                      pipeline_name, 
                      payload, 
                      duckdb_path, 
                      context: RequestContext):

    template = NodeFactory.new_node(fst_connection.get('name',None), None, context).template
    data_place, node_list = {}, []

    connections = context.connections 
    node_params = context.node_params

    all_nodes: list[TemplateNodeType] = parse_node(connections, node_params, data_place, context, node_list)
    template = template_final_parsing(template, pipeline_name, payload, duckdb_path, context)

    for data in data_place.items():
        value = data[1] if check_type(data[1]) else str(data[1])
        template = template.replace(data[0], str(value))

    if len(context.exceptions) > 0:
        message = list(context.exceptions[0].values())[0]['message']
        revert_and_notify_failure(None, all_nodes, message)
        print(f'TERMINATED WITH EXCEPTIONS: {context.exceptions}')
        return { 'error': True, 'result': context.exceptions }

    pipeline_instance, success = DltPipeline(), True

    ppline_path = context.ppline_path
    diagrm_path = context.diagrm_path
    pipeline_lbl = context.pipeline_lbl

    try:
        result = pipeline_instance.create_v1(ppline_path, pipeline_name, template, context)

        if(result['status'] == True):
            pipeline_instance.save_diagram(diagrm_path, pipeline_name, payload['drawflow'], pipeline_lbl)
        
        if result['status'] is False: 
            success, message = False, result['message'] 

        return { 'error': False, 'result': 'Pipeline created successfully' }
    except Exception as err:
        result = { 'message': str(err) }
        success, message = False, result['message']

    finally:
        if success is False:
            revert_and_notify_failure(pipeline_instance, all_nodes, message)

        return { 'error': False, 'result': message }


def create_new_version_ppline(fst_connection, 
                      pipeline_name, 
                      payload, 
                      duckdb_path, 
                      context: RequestContext):

    import re

    template = NodeFactory.new_node(fst_connection.get('name',None), None, context).template

    connections = context.connections 
    node_params = context.node_params
    ppline_path = context.ppline_path
    diagrm_path = context.diagrm_path
    pipeline_lbl = context.pipeline_lbl

    data_place, node_list = {}, []
    all_nodes: list[TemplateNodeType] = parse_node(connections, node_params, data_place, context, node_list)

    file_path = f'{ppline_path}/{pipeline_name}.py'
    pipeline_instance = DltPipeline()
    template = pipeline_instance.get_template_from_existin_ppline(file_path)

    pattern = r"# <transformation>.*?# </transformation>"
    replacement = "# <transformation>\n            %transformation%\n            # </transformation>"
    template = re.sub(pattern, replacement, template, flags=re.DOTALL)

    parse_transformation_task(node_params, context)
    transformation = context.transformation

    if(transformation is not None):
        template = template.replace('%transformation%',transformation)

    try:
        success = True
        result = pipeline_instance.create_v1(ppline_path, pipeline_name, template, context)
        if(result['status'] == True):
            pipeline_instance.save_diagram(diagrm_path, pipeline_name, payload['drawflow'], pipeline_lbl, True, False)
        
        if result['status'] is False: 
            success, message = False, result['message'] 

    except Exception as err:
        result = { 'message': err }
        success, message = False, result['message']

    finally:
        if success is False:
            revert_and_notify_failure(pipeline_instance, all_nodes, message)

        return { 'error': success, 'result': message }


def pepeline_init_param(payload):
    grid = payload['drawflow'] if 'drawflow' in payload else ''
    start_id = payload['startNode'] if 'startNode' in payload else ''
    node_params = grid['Home']['data']
    
    return start_id, node_params


def parse_transformation_task(node_params, context: RequestContext):
    
    """
    This code will be added to the pipeline as a transformation step
    and the pipeline will run in a separate process/subprocess, then
    the print statement is for it the send progress to main process
    """
    
    code = ''
    transformation_count = 0
    for id in node_params:
        if node_params[id]['name'] == 'Transformation':
            transformation_count = transformation_count + 1
            node_data = node_params[id]['data']
            code_lines = node_data['code'].split('\n')
            # Generate the transformation code line 
            # by line and assign to code variable
            line_counter = 0
            for line in code_lines:
                line_counter = line_counter + 1
                # Adds 12 spaces (3 tabs) in each line
                if line_counter == 1:
                    code += ' ' * 12  +line.replace('\n','').expandtabs(8)
                else:
                    code += '\n'+ ' ' * 12  +line.expandtabs(8)
            
            code += '\n'+ ' ' * 12 + f"# Bellow line is to notify the UI/Frontend that transformation step has completed"
            code += '\n'+ ' ' * 12 + f"print('{node_data['componentId']}', flush=True)"
            code += '\n'+ ' ' * 12 + f"print('Transformation #{transformation_count} process completed', flush=True)"
    
    context.transformation = None if code == '' else code
    return node_params


def handle_user_tenancy_folders(payload, context: RequestContext):
    duckdb_path = BasePipeline.folder+'/duckdb/'+payload['user']
    os.makedirs(duckdb_path, exist_ok=True)
    
    ppline_path = BasePipeline.folder+'/pipeline/'+payload['user']
    os.makedirs(ppline_path, exist_ok=True)
    
    diagrm_path = BasePipeline.folder+'/code/'+payload['user']
    os.makedirs(diagrm_path, exist_ok=True)
    
    context.user = payload['user']
    return duckdb_path, ppline_path, diagrm_path
    

def set_pipeline_name(payload):
    pipeline_name = payload['activeGrid'] if 'activeGrid' in payload else ''
    pipeline_lbl = payload['pplineLbl'] if 'pplineLbl' in payload else ''
    return pipeline_name, pipeline_lbl
    

def template_final_parsing(template, pipeline_name, payload, duckdb_path, context: RequestContext = None):
    template = template.replace('%pipeline_name%', f'"{pipeline_name}"').replace('%Usr_folder%',duckdb_path)
    template = template.replace('%Dbfile_name%', pipeline_name)
    template = template.replace('%User_folder%', payload['user'])
    if(context.transformation):
        template = template.replace('%transformation%', context.transformation)
    
    return template


def parse_node(connections, node_params, data_place, context: RequestContext, node_list: list):
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


def revert_and_notify_failure(
        pipeline_instance: DltPipeline = None, 
        all_nodes: list[TemplateNodeType] = None, 
        message = None):
    if(pipeline_instance is not None):
        pipeline_instance.revert_ppline()
    for node in all_nodes:
        node.notify_failure_to_ui('Pipeline', message,False)
        

@pipeline.route('/scriptfiles/<user>/')
def scriptfiles(user):

   def format_size(size_bytes):
       if size_bytes < 1024:
           return size_bytes, "bytes"
       elif size_bytes < 1024**2:
           return round(size_bytes/1024, 1), "KB"
       elif size_bytes < 1024**3:
           return round(size_bytes/(1024**2), 1), "MB"
       else:
           return round(size_bytes/(1024**3), 1), "GB"
   
   try:
        files_path = BasePipeline.folder+'/pipeline/'+user+'/'
        files = []
       
        for filename in os.listdir(files_path):
           filepath = os.path.join(files_path, filename)
           if os.path.isfile(filepath):
               size_bytes = os.path.getsize(filepath)
               size_value, size_unit = format_size(size_bytes)
               file_type = filepath.split('.')[-1]
               files.append({'name': filename, 'size': size_value, 'unit': size_unit, 'type': file_type})
       
        return jsonify(files)
   except FileNotFoundError as err:
       return jsonify({'error': 'User folder not found'}), 404


@pipeline.route('/scriptfiles/<user>/<filename>', methods=['GET'])
def read_scriptfiles(user, filename):

   try:
        file_path = BasePipeline.folder+'/pipeline/'+user+'/'+filename
        code = ''
        with open(file_path, 'r') as file:
            code = file.read()

        return code
   except FileNotFoundError as err:
       return jsonify({'error': 'Pipeline not found'}), 404


@pipeline.route('/scriptfiles/<user>/<filename>', methods=['POST'])
def update_ppline(user, filename):

    payload = request.get_data()
    pipeline_name = payload['activeGrid'] if 'activeGrid' in payload else ''
    pipeline_lbl = payload['pplineLbl'] if 'pplineLbl' in payload else ''
    ppline_path = BasePipeline.folder+'/pipeline/'+user

    _, node_params = pepeline_init_param(payload)
    context = RequestContext(pipeline_name, payload['socketSid'])
    
    pipeline_instance = DltPipeline()
    pipeline_instance.update_ppline(ppline_path, filename, payload, None)
    
    node_params = parse_transformation_task(node_params, context)

    diagrm_path = BasePipeline.folder+'/code/'+user
    pipeline_instance.save_diagram(diagrm_path, pipeline_name, payload['drawflow'], pipeline_lbl)
    
    return ''


   
@pipeline.route('/ppline/diagram/<user>/<filename>', methods=['GET'])
def read_diagram_content(user, filename):

   try:
        file_path = BasePipeline.folder+'/code/'+user+'/'+filename+'.json'
        code = ''
        with open(file_path, 'r') as file:
            code = file.read()

        return code
   except FileNotFoundError as err:
       return jsonify({'error': 'Pipeline not found'}), 404
   
   
@pipeline.route('/ppline/data/csv/<user>/<filename>')
def read_csv_file_fields(user, filename):
    from .file_upload import BaseUpload
    file_path = BaseUpload.upload_folder+'/'+user+'/'+filename
    
    df = pd.read_csv(file_path, nrows=1)
    return str(df.columns)
    