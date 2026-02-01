"""
Pipeline logging configuration for dltHub pipeline execution.

This module provides configuration management for pipeline-specific logging setup,
including dltHub logging configuration and context management.
"""
import logging
import json

    

class PipelineLogger:

    def __init__(self):
        self.setup_dlt_logging()

    def info(self, description, extra=None):
        print(f'{description} |+| {json.dumps(extra)}')

    def error(self, description, extra=None, exc_info=None):
        print(f'ERROR: {description} |+| {json.dumps(extra)}')

    def debug(self, description, extra=None):
        print(f'DEBUG: {description} |+| {json.dumps(extra)}')

    def setup_dlt_logging(self):

        dlt_logger = logging.getLogger('dlt')
        dlt_logger.setLevel(logging.DEBUG)

        dlt_logger_handler = logging.StreamHandler()
        dlt_logger_handler.setFormatter(logging.Formatter(f'%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        dlt_logger.addHandler(dlt_logger_handler)

def handle_pipeline_log(printed_log: str, logger: logging.Logger):
    
    # logs application level implemented logs 
    if str(printed_log).__contains__(' |+| '):
        description,\
        extra = printed_log.replace('[PIPELINE_LOG]:','').split(' |+| ')
        if description.strip() != '':
            logger.info(description, extra=json.loads(extra))
    # logs details logs generated from DLT library 
    else:
        if printed_log.strip() != '':
            logger.info(printed_log)
    