from flask import Flask, request
from services.pipeline.DltPipeline import DltPipeline

app = Flask(__name__)

@app.route('/', methods=["POST"])
def main():
    """
    This is pipeline creation request handler
    """
    data = request.get_json()
    DltPipeline.create(data)
    return "Pipeline created successfully"