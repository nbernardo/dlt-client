from flask import Blueprint, request, jsonify
from services.admin.MappingTemplateService import MappingTemplateService
from services.admin.DataFlowService import DataFlowService
from services.pipeline.OperationPipeline import prepare_context
from controller.pipeline import parse_node
from controller.pipeline import BasePipeline
import os
from cryptography.fernet import Fernet
import base64
import hashlib
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import json
from services.pipeline.DltPipeline import DltPipeline
from utils.logging.pipeline_logger_config import handle_pipeline_log

RAW_KEY = os.environ.get("SQLITE_DB_ENCRYPTION_KEY")
ENCRYPTION_KEY = hashlib.sha256(RAW_KEY.encode('utf-8')).digest()
cipher = Fernet(base64.urlsafe_b64encode(ENCRYPTION_KEY))

aesgcm = AESGCM(ENCRYPTION_KEY)

import base64
import hashlib
from cryptography.fernet import Fernet


def hook_decrypt_secret(encrypted_text: str, raw_key = None) -> str:
    global RAW_KEY
    if not encrypted_text:
        return ""
    raw_key = raw_key or RAW_KEY
    digest = hashlib.sha256(raw_key.encode("utf-8")).digest()
    cipher = Fernet(base64.urlsafe_b64encode(digest))
    return (cipher.decrypt(encrypted_text.strip().encode("utf-8")).decode("utf-8") or '').replace("'",'"')


template = Blueprint('template', __name__)

@template.route('/template/<namespace>', methods=['POST'])
def create_template(namespace):
    payload = request.get_json()

    record = [{ 
        'template_type': payload.get('type'), 'template_content': payload.get('xmlConfig'), 
        'connection': payload.get('connectionName'),'name': payload.get('templateName'),
        'query': payload.get('dataQuery'), 'yml_content': payload.get('yamlConfig')
    }]
    result = MappingTemplateService.create_template_records(record, namespace)

    return { 'success': True if result.get('error') == None else False, 'result': result }


@template.route('/template/<namespace>', methods=['GET'])
def get_template(namespace):
    
    result = MappingTemplateService.get_template_records(namespace)
    return { 'success': True if result.get('error') == None else False, 'result': result }


@template.route('/hook/<namespace>', methods=['GET'])
def get_hook_endpoint(namespace):
    import uuid
    return { 'success': True, 'result': uuid.uuid4() }



@template.route("/hook", methods=["POST"])
@template.route("/hook/<namespace>/<hook_endpoint>", methods=["POST"])
def receive_file(namespace = None, hook_endpoint = None):
    
    if(request.authorization):
        auth = request.authorization.parameters
        creds = { "user": auth.get('username'), "pass": auth.get('password') }
        
    else:
        return jsonify({ 'status': 'error', 'message': f'User credentials (user and/or password) are missing' }), 400
    
    if "file" not in request.files:
        return jsonify({"error": "No 'file' part in request"}), 400

    pipeline_details, context = None, None
    if namespace and hook_endpoint:

        uploaded_file = request.files["file"]
        if not uploaded_file.filename:
            return jsonify({"error": "Empty filename"}), 400
        
        full_endpoint = f' /{namespace}/{hook_endpoint}'
        pipeline_details = DataFlowService.get_data_flow_by_hook(full_endpoint)
        
        if(len(pipeline_details.get('result')) < 1):
            return jsonify({ 'status': 'error', 'message': f'Hook end point ending in "{full_endpoint}" does not exist' }), 400
        
        saved_creds = hook_decrypt_secret(pipeline_details.get('result')[0][3])
        saved_creds = json.loads(saved_creds)

        if creds.get('user') != saved_creds.get('user') \
            or creds.get('pass') != saved_creds.get('pass'):
            return jsonify({ 'status': 'error', 'message': f'User credentials "{full_endpoint}" does not exist' }), 401

        file_dir = BasePipeline.folder+'/operations/received-files/'+namespace
        os.makedirs(file_dir, exist_ok=True)

        dest_path = os.path.join(file_dir, uploaded_file.filename)
        uploaded_file.save(dest_path)
        

        context = prepare_context(pipeline_details, namespace)
        context.operational_metadata.input_file = dest_path

        logger = DltPipeline.get_pipeline_logger(context)
        context.operational_metadata.logger = logger
        handle_pipeline_log(f"[FILE RECEIVED] Saved '{uploaded_file.filename}' to '{dest_path}'", logger, False)

        parse_node(context.connections, context.node_params, {}, context, [])

    return jsonify({'status': 'ok', 'filename': uploaded_file.filename}), 200