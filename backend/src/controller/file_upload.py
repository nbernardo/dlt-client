from flask import Blueprint, request, jsonify
from pathlib import Path
from services.workspace.Workspace import Workspace
import os

upload = Blueprint('upload', __name__)

class BaseUpload:
    upload_folder = None

ALLOWED_EXTENSIONS = {'csv', 'xml', 'json', 'xls', 'xlsx'}
MAX_FILE_SIZE = 16 * 1024 * 1024  # 16MB

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@upload.route('/upload', methods=['POST'])
def upload_files():
    uploaded_files = []
    user = request.form['user']
    files_path = BaseUpload.upload_folder+'/'+user
    os.makedirs(files_path, exist_ok=True)
    if 'files' not in request.files:
        return jsonify({'error': 'No files selected'}), 400
    
    files = request.files.getlist('files')
    
    for file in files:
        if file.filename != '' and allowed_file(file.filename):
            filename = file.filename
            filepath = os.path.join(files_path, filename)
            file.save(filepath)
            uploaded_files.append({
                'filename': filename,
                'filepath': filepath
            })
    
    if uploaded_files:
        return jsonify({
            'message': f'{len(uploaded_files)} files uploaded successfully',
            'files': uploaded_files
        })
    
    return jsonify({'error': 'No valid files uploaded'}), 400



@upload.route('/files/<user>')
def list_files(user):

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
        files_path = BaseUpload.upload_folder+'/'+user
        files = []
       
        for filename in os.listdir(files_path):
           filepath = os.path.join(files_path, filename)
           if os.path.isfile(filepath):
               size_bytes = os.path.getsize(filepath)
               size_value, size_unit = format_size(size_bytes)
               file_type = filepath.split('.')[-1]
               files.append({'name': filename, 'size': size_value, 'unit': size_unit, 'type': file_type})
       
        return jsonify(files)
   except FileNotFoundError:
       return jsonify({'error': 'User folder not found'}), 404
