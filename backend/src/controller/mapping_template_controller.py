from os import getenv as env
from flask import Blueprint, request, jsonify
from services.admin.MappingTemplateService import MappingTemplateService

template = Blueprint('template', __name__)


@template.route('/template/<namespace>', methods=['POST'])
def create_template(namespace):
    payload = request.get_json()

    record = [{ 
        'template_type': payload.get('type'), 
        'template_content': payload.get('xmlConfig'), 
        'connection': payload.get('connectionName'),
        'name': payload.get('templateName'),
    }]
    result = MappingTemplateService.create_template_records(record, namespace)

    return { 'success': True if result.get('error') == None else False, 'result': result }

