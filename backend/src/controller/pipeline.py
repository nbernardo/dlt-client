from flask import Blueprint, request, jsonify
from services.pipeline.DltPipeline import DltPipeline, create_execution_id
from node_mapper.NodeFactory import NodeFactory
from .RequestContext import RequestContext
from node_mapper.TemplateNodeType import TemplateNodeType
import os
import pandas as pd
from utils.duckdb_util import DuckdbUtil
from utils.workspace_util import handle_conversasion_turn_limit
import traceback

escape_component_field = ['context', 'component_id','template']
pipeline = Blueprint('pipeline', __name__)

class BasePipeline:
    folder = None
    file_folder_map = { 'data':'dbs/files', 'pipeline': 'destinations/pipeline'}


@pipeline.route('/pipeline/create', methods=['POST','PUT'])
def create():
    """ This is pipeline creation request handler """
    
    payload = request.get_json()
    
    pipeline_name, pipeline_lbl = set_pipeline_name(payload)
    context = RequestContext(pipeline_name, payload['socketSid'])
    context.action_type = 'UPDATE' if request.method == 'PUT' else None
    context.is_code_destination = payload['codeOutput']
    context.is_duck_destination = payload['duckOutput']

    if('actionType' in payload):
        context.pipeline_action = payload['actionType']

    duckdb_path, ppline_path, diagrm_path = handle_user_tenancy_folders(payload, context)
    start_node_id, node_params, sql_destinations = pepeline_init_param(payload)
    node_params = parse_transformation_task(node_params, context)

    start_node = node_params.get(f'{start_node_id}')
    connections = start_node.get('outputs').get('output_1').values()
    
    if(len(list(connections)[0]) == 0):
        return { 'error': True, 'result': 'Please connect all nodes accordingly.' }
    
    first_connection_id = list(connections)[0][0]['node']
    fst_connection = node_params.get(first_connection_id)

    is_cloud_bucket_req = False
    if 'bucketFileSource' in fst_connection['data']:
        is_cloud_bucket_req = int(fst_connection['data']['bucketFileSource']) == 2

    context.ppline_path = ppline_path
    context.diagrm_path = diagrm_path
    context.pipeline_lbl = pipeline_lbl
    context.pipeline_name = pipeline_name
    context.connections = connections
    context.node_params = node_params
    context.sql_destinations = sql_destinations
    context.sql_dest = payload['sqlDest']
    context.is_cloud_url = True if is_cloud_bucket_req else False
    context.has_cloud_bucket_auth = ''

    if 's3Auth' in payload:
        context.has_cloud_bucket_auth = 's3Auth' if payload['s3Auth'] else ''
        
    context.code_source = payload['codeInput']
    context.pipeline_execution_id = create_execution_id()

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

    template_params: dict = {}

    if type(payload) == dict:
        if 'initDbengine' in payload:
            template_params['dbengine'] = payload['initDbengine']
        if 'isOldSQLNode' in payload:
            template_params['old_template'] = payload['isOldSQLNode']

    fst_node_component_id = fst_connection.get('data',{}).get('componentId',None)
    
    if(fst_connection.get('name',None) == 'InputAPI'):
        template_params = { **template_params, **fst_connection.get('data',{}) }

    template = NodeFactory.new_node(fst_connection.get('name',None), template_params, context, fst_node_component_id).template
    data_place, node_list = {}, []

    connections = context.connections 
    node_params = context.node_params
    message = ''

    all_nodes: list[TemplateNodeType] = parse_node(connections, node_params, data_place, context, node_list)
    template = template_final_parsing(template, pipeline_name, payload, duckdb_path, context)

    for data in data_place.items():
        value = data[1] if check_type(data[1]) else str(data[1])
        template = template.replace(data[0], str(value))

    template = parse_secrets(template, context)

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

        return { 'error': not(success), 'result': 'Pipeline created successfully' }
    except Exception as err:
        result = { 'message': str(err) }
        success, message = False, result['message']

    finally:
        if success is False:
            revert_and_notify_failure(pipeline_instance, all_nodes, message)

        return { 'error': not(success), 'result': message }


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

    DuckdbUtil.check_pipline_db(f'{duckdb_path}/{pipeline_name}.duckdb')

    transformation = context.transformation
    message = ''

    if(transformation is not None):
        template = template.replace('%transformation%',transformation)

    try:
        success = True
        result = pipeline_instance.create_v1(ppline_path, pipeline_name, template, context)
        if(result['status'] == True):
            pipeline_instance.save_diagram(diagrm_path, pipeline_name, payload['drawflow'], pipeline_lbl, True, False)
        
        if result['status'] is False: 
            print('Error while creating new pipeline version: ', str(result['message']))
            success, message = False, result['message'] 

    except Exception as err:
        print('Exception while creating new pipeline version: ', str(result['message']))
        result = { 'message': err }
        success, message = False, result['message']

    finally:
        if success is False:
            revert_and_notify_failure(pipeline_instance, all_nodes, message)

        return { 'error': success, 'result': message }


def pepeline_init_param(payload):
    grid = payload['drawflow'] if 'drawflow' in payload else ''
    start_id = payload['startNode'] if 'startNode' in payload else ''
    sql_destinations = payload['sqlDestinations'] if 'sqlDestinations' in payload else ''
    node_params = grid['Home']['data']
    #sql_source = payload['sqlSource']
    
    return start_id, node_params, sql_destinations


def parse_transformation_task(node_params, context: RequestContext):
    
    """
    This code will be added to the pipeline as a transformation step
    and the pipeline will run in a separate process/subprocess, then
    the print statement is for it the send progress to main process
    """
    
    code = ''
    context.transformation_type = None

    transformation_count = 0
    for id in node_params:
        if node_params[id]['name'] == 'Transformation':
            
            transformation_count = transformation_count + 1
            node_data = node_params[id]['data']
            source_type = node_data['dataSourceType']
            transformation_str, transformation_str2 = None, None

            if source_type == 'SQL':
                context.transformation_ui_node_id = node_data['componentId']
                context.transformation_type = source_type
                all_tables = list(node_data['code'].keys()) + list(node_data['code2'].keys())
                tables = []
                [tables.append(table) for table in all_tables if table not in tables]
                
                transformation_str = '{'
                transformation_str2 = ''

                for table in tables:

                    if(table in node_data['code']):
                        pl_script = str(node_data['code'][table])\
                                        .replace('["','|inBracket|')\
                                        .replace('"]','|outBracket|')+','
                        
                        transformation_str += f"\n'{table}': {pl_script}"
                    
                    if(table in node_data['code2']):
                        pl_script2 = str(node_data['code2'][table])\
                                        .replace('["','|inBracket|')\
                                        .replace('"]','|outBracket|')+','
                        
                        transformation_str2 += f"\n'{table}': {pl_script2}"
                    
                transformation_str = transformation_str\
                                            .replace('|inBracket|','[')\
                                            .replace('|outBracket|',']')\
                                            .replace(')"',')')\
                                            .replace('"pl','pl')
                
                transformation_str2 = transformation_str2\
                                            .replace('|inBracket|','[')\
                                            .replace('|outBracket|',']')\
                                            .replace(')"',')')\
                                            .replace('"pl','pl')\
                                            .replace('"lambda df','lambda df') #TODO: Improve this from UI. This is a workareound for the transformation2
                                            
            else:
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
            
            if transformation_str != None:
                transformation_str = transformation_str[0:-1] + '\n}'
                context.transformation = transformation_str
            
            if transformation_str2 != None:
                transformation_str2 = '{'+ transformation_str2 + '\n}'
                context.transformation2 = transformation_str2

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

def handle_transform_indent(transformation: str):
    return transformation\
                        .replace('\n','\n\t\t\t\t')\
                        .replace('\n\t\t\t\t{','\n\t\t\t{')\
                        .replace('\t}','}')


def template_final_parsing(template, pipeline_name, payload, duckdb_path, context: RequestContext = None):
    template = template.replace('%pipeline_name%', f'"{pipeline_name}"').replace('%Usr_folder%',duckdb_path)
    template = template.replace('%Dbfile_name%', pipeline_name)
    template = template.replace('__current.PIPELINE_NAME', f"'{pipeline_name}'")
    template = template.replace('%User_folder%', payload['user'])
    template = template.replace('%namespace%', f"'{payload['user']}'")
    # %table_format% replace might be preceeded by the DLTCodeOutput node type which
    # means that if this was stated at the node level, this one won't take any effect 
    template = template.replace('%table_format%', '')
    
    if(context.transformation):
        transformation = context.transformation.replace('"(pl.','(pl.')
        if(context.source_type == 'BUCKET'):
            transformation = f'transformation = {transformation}'
            transformation = handle_transform_indent(transformation)
            
        template = template.replace('%transformation%', transformation)
    
    placeholder = '%transformation2%'
    if(context.transformation2):
        transformation2 = context.transformation2
        if(context.source_type == 'BUCKET'):
            t = '    '
            transformation2 = context.transformation2
            transformation2 = handle_transform_indent(transformation2)
            transformation2 = f'{transformation2}\n{t}{t}{t}transformations2 = list(transformations2.values())[0]'
            
        template = template.replace(placeholder, f'transformations2 = {transformation2}')
    else:
        template = template.replace(placeholder, 'transformations2 = []')
    
    return template


def parse_secrets(template: str, context: RequestContext = None):

    has_metadata = context.is_code_destination and len(context.sql_destinations) > 0
    if(has_metadata == False):
        has_metadata = context.code_source and context.additional_secrets != None

    if has_metadata:
        template = f'# METADATA: dest_tables=[]\n{template}'
        return template.replace('%referenced_secrets_list%', str(context.additional_secrets).replace('"',''))
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

                        if 'parse_to_literal' in node.__dict__:
                            if node.__dict__['parse_to_literal'].__contains__(field):
                                data_place[f'%{field}%'] = data
                            else:
                                data_place[f'%{field}%'] = f'"{data}"' if type(data) == str else data
                        else:
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
           if str(filename).endswith('.meta'): 
               continue
           filepath = os.path.join(files_path, filename)
           if os.path.isfile(filepath):
               size_bytes = os.path.getsize(filepath)
               size_value, size_unit = format_size(size_bytes)
               file_type = filepath.split('.')[-1]
               files.append({'name': filename, 'size': size_value, 'unit': size_unit, 'type': file_type})
       
        files.sort(key=lambda x: x['name'])
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

    _1, node_params, _2 = pepeline_init_param(payload)
    context = RequestContext(pipeline_name, payload['socketSid'])
    
    pipeline_instance = DltPipeline()
    pipeline_instance.update_ppline(ppline_path, filename, payload, None)
    
    node_params = parse_transformation_task(node_params, context)

    diagrm_path = BasePipeline.folder+'/code/'+user
    pipeline_instance.save_diagram(diagrm_path, pipeline_name, payload['drawflow'], pipeline_lbl)
    
    return ''


   
@pipeline.route('/ppline/diagram/<namespace>/<filename>', methods=['GET'])
def read_diagram_content(namespace, filename):

   try:
        file_path = BasePipeline.folder+'/code/'+namespace+'/'+filename+'.json'
        pipeline_code, datasource_details = DltPipeline.read_pipeline(file_path, namespace)
        return { 'pipelineCode': pipeline_code , 'dbDetailes': datasource_details }
   
   except FileNotFoundError as err:
       return jsonify({'error': 'Pipeline not found'}), 404
   
   
@pipeline.route('/ppline/data/csv/<user>/<filename>')
def read_csv_file_fields(user, filename: str):
    from .file_upload import BaseUpload
    file_path = BaseUpload.upload_folder+'/'+user+'/'+filename
    
    if(filename.lower().endswith('csv')):
        df = pd.read_csv(file_path, nrows=1)
        return str(df.columns)

    import polars as pl
    if(filename.lower().endswith('parquet')):
        return list(pl.scan_parquet(file_path).collect_schema().keys())

    if(filename.lower().endswith('jsonl')):
        return list(pl.scan_ndjson(file_path).collect_schema().keys())

    


from services.agents import AgentFactory

def send_message_to_pipeline_agent_wit_groq(message, namespace, user_id = None):
    user = user_id if user_id != None else namespace
    agent = AgentFactory.get_pipeline_agent(user)

    if(agent == None):
        return { 
            'success': False, 
            'result': f"Error while starting Pipeline agent",
            'started': False
        }

    return { 'success': True, 'result': agent.cloud_groq_call(message) }


@pipeline.route('/pipeline/agent/<namespace>', methods=['POST'])
def message_ai_agent(namespace):

    try:

        message_turn_limit = handle_conversasion_turn_limit(request, namespace)
        if message_turn_limit.get('error', None):
            return message_turn_limit

        payload = request.get_json()
        message = payload['message']

        if(os.path.exists(namespace)):
            result = 'No agent was started since no data found in the Namespace.'
            return { 'error': False, 'result': { 'result': result } }
        
        return send_message_to_pipeline_agent_wit_groq(message, namespace)
    except Exception as error:
        print(f'AI Agent error while processing your request {str(error)}')
        print(error)
        traceback.print_exc()
        result = 'No medatata was loaded about your namespace.'\
              if str(error).strip() == namespace else 'Could not load details about your namespace.'
        return { 'error': True, 'result': { 'result': result } }


@pipeline.route('/<namespace>/db/transformation/preview', methods=['POST'])
def preview_transformation(namespace):

    try:

        payload = request.get_json()
        
        preview_script = payload['previewScript']
        source_type = payload['sourceType']

        if(source_type == 'BUCKET'):
            base_path = str(BasePipeline.folder).replace('/destinations','')
            fiile_path = f'{base_path}/{BasePipeline.file_folder_map['data']}/{namespace}/'
            preview_script = preview_script.replace('%pathToFile%/',fiile_path)
            preview_result = DltPipeline.get_file_data_transformation_preview(preview_script)
        else:
            connection_name = payload['connectionName']
            dbengine = payload['dbEngine']
            preview_result = DltPipeline.get_sqldb_transformation_preview(
                namespace, dbengine,connection_name, preview_script
            )
        return preview_result
    
    except Exception as err:
        error = f'Error while running transformation preview {str(err)}'
        print(error)
        traceback.print_exc()
        return { 'error': True, 'result': { 'result': err } }
    

@pipeline.route('/ppline/schedule/<namespace>/<pipeline>/<status>', methods=['POST'])
@pipeline.route('/ppline/schedule/<namespace>/<pipeline>/<status>/', methods=['POST'])
def update_pipeline_pause(namespace, pipeline, status):
    try:
        DltPipeline.update_pipline_pause_status(namespace, pipeline, status)
        return { 'error': False, 'result': { 'result': 'Pipeline job paused' } }
    except Exception as err:
        return { 'error': True, 'result': { 'result': err } }

