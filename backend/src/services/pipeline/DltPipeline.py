import os
import subprocess
import duckdb
import json
from controller.RequestContext import RequestContext
from pathlib import Path
from typing import Dict, Optional
from node_mapper.Transformation import Transformation
from utils.FileVersionManager import FileVersionManager
from utils.duckdb_util import DuckdbUtil
from utils.cache_util import DuckDBCache
from utils.SQLDatabase import SQLDatabase
import uuid
from datetime import datetime
import time
from utils.code_node_util import valid_imports, FORBIDDEN_CALLS, FORBIDDEN_CALLS_REGEX, FORBIDDEN_DUNDER_REGEX
import schedule
import logging

# Enhanced logging imports
from utils.pipeline_logger_config import PipelineLoggerConfig, configure_pipeline_logging
from utils.log_storage import DuckDBLogStore
from utils.log_processor import initialize_log_processor, get_log_processor
from utils.logging_models import create_execution_id

root_dir = str(Path(__file__).parent.parent.parent.parent)
destinations_dir = f'{root_dir}/destinations/pipeline'
template_dir = f'{root_dir}/src/pipeline_templates'


class DltPipeline:
    """
    This is the class to create and handle pipelines
    """
    def __init__(self):
        self.curr_file = None
        self.logger = logging.getLogger(__name__)
        self._pipeline_logger_config: Optional[PipelineLoggerConfig] = None
        
        # Initialize enhanced logging system if not already done
        self._ensure_logging_system_initialized()
    
    def _ensure_logging_system_initialized(self):
        """Ensure the enhanced logging system is initialized."""
        try:
            processor = get_log_processor()
            if processor is None:
                # Initialize with DuckDB storage
                storage = DuckDBLogStore()
                storage.initialize_tables()
                initialize_log_processor(storage)
                self.logger.info("Enhanced logging system initialized")
        except Exception as e:
            self.logger.warning(f"Failed to initialize enhanced logging system: {e}")
    
    def _setup_pipeline_logging(self, pipeline_name: str, namespace: str = None, 
                               user_id: str = None, template_type: str = None) -> str:
        """
        Set up enhanced logging for pipeline execution.
        
        Args:
            pipeline_name: Name of the pipeline
            namespace: Optional namespace
            user_id: Optional user identifier
            template_type: Optional template type
            
        Returns:
            Execution ID for correlation
        """
        try:
            # Generate correlation ID for this execution
            execution_id = create_execution_id()
            
            # Create pipeline logger configuration
            self._pipeline_logger_config = PipelineLoggerConfig(
                pipeline_id=pipeline_name,
                log_level="INFO",
                namespace=namespace or "default"
            )
            
            # Configure dltHub logging
            self._pipeline_logger_config.configure_dlt_logging(
                execution_id=execution_id,
                user_id=user_id,
                pipeline_name=pipeline_name,
                template_type=template_type
            )
            
            # Log pipeline execution start
            pipeline_logger = self._pipeline_logger_config.get_logger(f"pipeline.{pipeline_name}")
            pipeline_logger.info(f"Starting pipeline execution: {pipeline_name}", extra={
                'execution_id': execution_id,
                'pipeline_name': pipeline_name,
                'namespace': namespace,
                'user_id': user_id,
                'template_type': template_type
            })
            
            return execution_id
            
        except Exception as e:
            self.logger.error(f"Failed to setup pipeline logging: {e}")
            # Return a fallback execution ID
            return create_execution_id()
    
    def _cleanup_pipeline_logging(self):
        """Clean up pipeline logging resources."""
        try:
            if self._pipeline_logger_config:
                self._pipeline_logger_config.cleanup()
                self._pipeline_logger_config = None
        except Exception as e:
            self.logger.error(f"Error during pipeline logging cleanup: {e}")
    
    def _log_pipeline_event(self, level: str, message: str, **kwargs):
        """
        Log a pipeline event with enhanced logging.
        
        Args:
            level: Log level (INFO, WARNING, ERROR, etc.)
            message: Log message
            **kwargs: Additional context
        """
        try:
            if self._pipeline_logger_config:
                logger = self._pipeline_logger_config.get_logger("pipeline.execution")
                log_method = getattr(logger, level.lower(), logger.info)
                log_method(message, extra=kwargs)
        except Exception as e:
            self.logger.debug(f"Failed to log pipeline event: {e}")
    
    def _get_pipeline_name_from_context(self, context: RequestContext) -> str:
        """Extract pipeline name from context."""
        if hasattr(context, 'pipeline_name') and context.pipeline_name:
            return context.pipeline_name
        elif hasattr(context, 'ppline_name') and context.ppline_name:
            return context.ppline_name
        else:
            return "unknown_pipeline"
    
    def _get_namespace_from_context(self, context: RequestContext) -> str:
        """Extract namespace from context."""
        if hasattr(context, 'namespace'):
            return context.namespace
        return "default"


    def create(self, data):
        """
        This is the pipeline creation
        """
        file_name = data['pipeline']

        file_path = f'{destinations_dir}/{file_name}.py'
        file_open_flag = 'x+'
        
        template = DltPipeline.get_template()

        if int(data['bucketFileSource']) == 2:
            template = DltPipeline.get_s3_no_auth_template()

        if os.path.exists(file_path):
            return 'Pipeline exists already'
            
        with open(file_path, file_open_flag, encoding='utf-8') as file:
            for field in data.keys():
                template = template\
                    .replace(f'%{field}%', f'"{data[field]}"')

            file.write(template)

        result = subprocess.run(['python', file_path],
                                check=True,
                                capture_output=True,
                                text=True)

        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout)
        print("Standard Error:", result.stderr)



    def create_v1(self, file_path, file_name, data, context: RequestContext = None) -> Dict[str,str]:
        """
        This is the pipeline new version creation
        """
        # Set up enhanced logging
        pipeline_name = self._get_pipeline_name_from_context(context) if context else file_name
        namespace = self._get_namespace_from_context(context) if context else "default"
        execution_id = self._setup_pipeline_logging(
            pipeline_name=pipeline_name,
            namespace=namespace,
            user_id=getattr(context, 'user', None),
            template_type="create_v1"
        )
        
        try:
            self._log_pipeline_event("info", f"Starting pipeline creation: {pipeline_name}", 
                                   execution_id=execution_id, action="create_v1")
            
            try:
                check_invalid_code(data)
            except RuntimeError as err:
                self._log_pipeline_event("error", f"Invalid code detected: {err}", 
                                       execution_id=execution_id, error_type="security")
                return { 'error': True, 'status': False, 'message': str(err) }
            
            is_sql_destination = len(context.sql_destinations) > 0 if context else False
            is_code_to_code_ppline = (context.code_source and context.is_code_destination) if context else False
            does_have_metadata = is_sql_destination == True or is_code_to_code_ppline == True

            if not(does_have_metadata):
                does_have_metadata = True if (context and context.bucket_source and context.is_code_destination) else False

            filename_suffixe = ''
            if context and context.is_duck_destination != True:
                filename_suffixe = '|withmetadata|' if does_have_metadata or context.is_code_destination else ''
                if(filename_suffixe == ''):
                    filename_suffixe = '|toschedule|' if context.pipeline_action == 'onlysave' else ''

            ppline_file = f'{file_path}/{file_name}{filename_suffixe}.py'
            file_open_flag = 'x+'
            
            self.curr_file = ppline_file
            
            self._log_pipeline_event("info", f"Creating pipeline file: {ppline_file}", 
                                   execution_id=execution_id, file_path=ppline_file)
            
            if context and context.action_type == 'UPDATE':
                ppline_file = DltPipeline.create_new_pipline_version(file_name, file_path, data)
                self._log_pipeline_event("info", f"Created new pipeline version: {ppline_file}", 
                                       execution_id=execution_id, action="update")
            else:
                # Create python file with pipeline code
                with open(ppline_file, file_open_flag, encoding='utf-8') as file:                    
                    file.write(data)
                self._log_pipeline_event("info", "Pipeline file created successfully", 
                                       execution_id=execution_id)

            if context and context.pipeline_action == 'onlysave':
                self._log_pipeline_event("info", "Pipeline saved without execution", 
                                       execution_id=execution_id, action="save_only")
                context.emit_ppsuccess()
                return { 'status': True, 'message': 'Pipeline created successfully' }

            # Run pipeline generater above by passing the python file
            self._log_pipeline_event("info", f"Starting pipeline execution: {ppline_file}", 
                                   execution_id=execution_id, action="execute")
            
            result = subprocess.Popen(['python', ppline_file],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        text=True,
                                        bufsize=1
                                    )
            pipeline_exception = False

            # TODO: If needed, flag can be assigned with proper logic so UI logs will only came in 
            #  specific situation like will only print if the ppline has transformation or if it's
            #  ppline update, otherwise flag = True will print the log in any scenario
            #  flag = context.transformation is not None or context.action_type == 'UPDATE'
            flag = True

            if(flag):
                while True:
                    line = result.stdout.readline()
                    time.sleep(0.1)

                    if not line:
                        break
                    line = line.strip()
                    
                    # Log each pipeline output line
                    self._log_pipeline_event("debug", f"Pipeline output: {line}", 
                                           execution_id=execution_id, output_line=line)
                    
                    is_transformation_step = (line.endswith('Transformation')\
                                               and line.startswith('dynamic-_cmp'))
                    
                    if (line == 'RUN_SUCCESSFULLY'):
                        self._log_pipeline_event("info", "Pipeline execution completed successfully", 
                                               execution_id=execution_id, status="success")
                        if context:
                            context.emit_ppsuccess()
                        pipeline_exception = False

                    else:
                        if(is_transformation_step and pipeline_exception == False):
                            component_ui_id = line
                            self._log_pipeline_event("info", f"Transformation step completed: {component_ui_id}", 
                                                   execution_id=execution_id, component_id=component_ui_id)
                            if context:
                                Transformation(None, context, component_ui_id).notify_completion_to_ui()
                            
                        elif(line.startswith('RUNTIME_ERROR:') or pipeline_exception == True):
                            pipeline_exception = True
                            error_message = line.replace('RUNTIME_ERROR:','')
                            self._log_pipeline_event("error", f"Runtime error: {error_message}", 
                                                   execution_id=execution_id, error_type="runtime")
                            if context:
                                context.emit_ppline_trace(error_message, error=True)

                        elif(line.startswith('RUNTIME_WARNING:')):
                            warning_message = line.replace('RUNTIME_WARNING:','')
                            self._log_pipeline_event("warning", f"Runtime warning: {warning_message}", 
                                                   execution_id=execution_id, warning_type="runtime")
                            if context:
                                context.emit_ppline_trace(warning_message, warn=True)
                        else:
                            if context:
                                context.emit_ppline_trace(line)
                
            result.kill()

            if pipeline_exception == True:
                self._log_pipeline_event("error", "Pipeline execution failed due to runtime errors", 
                                       execution_id=execution_id, status="failed")
                return { 'status': False, 'message': 'Runtime Pipeline error, check the logs for details' }

            message, status = 'Pipeline run terminated successfully', True
            
            error_messages, warning_status = None, False
            if result.returncode != 0 and not(context and context.action_type == 'UPDATE' and result.returncode == 2):
                error_messages = result.stderr.read().split('\n')
                if(str(error_messages).__contains__('[WARNING]')):
                    self._log_pipeline_event("warning", f"Pipeline completed with warnings: {error_messages}", 
                                           execution_id=execution_id, warnings=error_messages)
                    if context:
                        context.emit_ppline_trace(error_messages, warn=True)
                        context.emit_ppsuccess()
                    warning_status = True
                else:
                    message, status = '\n'.join(error_messages[1:]), False
                    self._log_pipeline_event("error", f"Pipeline execution failed: {message}", 
                                           execution_id=execution_id, error_messages=error_messages)
                    if context:
                        context.emit_ppline_trace(message, error=True)

            if context:
                context.emit_ppline_trace('PIPELINE COMPLETED SUCCESSFULLY')
            
            self._log_pipeline_event("info", f"Pipeline execution finished with status: {status}", 
                                   execution_id=execution_id, final_status=status, return_code=result.returncode)
            
            print("Return Code:", result.returncode)
            print("Standard Output:", result.stdout.read())
            print("Standard Error:", message if error_messages != None else None)

            if (error_messages != None or result.returncode == 1) and warning_status == False:
                status = False
            else:
                status = status if len(result.stderr.read()) > 0 else True

            return { 'status': status, 'message': message }
            
        except Exception as e:
            self._log_pipeline_event("error", f"Unexpected error during pipeline creation: {e}", 
                                   execution_id=execution_id, exception=str(e))
            return { 'error': True, 'status': False, 'message': f'Unexpected error: {str(e)}' }
        finally:
            # Clean up logging resources
            self._cleanup_pipeline_logging()


    def save_diagram(self, diagrm_path, file_name, content, pipeline_lbl, is_update = None, write_log = True):
        """
        Save pipeline diagram with enhanced logging.
        """
        # Set up minimal logging for diagram operations
        execution_id = create_execution_id()
        
        try:
            self.logger.info(f"Saving pipeline diagram: {file_name}", extra={
                'execution_id': execution_id,
                'diagram_path': diagrm_path,
                'is_update': is_update,
                'action': 'save_diagram'
            })
            
            # Create the pipeline Diagram code
            pipeline_code = { 'content': content, 'pipeline_lbl': pipeline_lbl }
            if is_update == True:
                file_manager = FileVersionManager(diagrm_path)
                file_manager.save_version(
                    f'{diagrm_path}/{file_name}.json', json.dumps(pipeline_code), 'updating transformation', write_log
                )
                self.logger.info(f"Updated pipeline diagram version: {file_name}", extra={
                    'execution_id': execution_id,
                    'action': 'diagram_version_update'
                })
            else:
                diagrm_file, file_open_flag = f'{diagrm_path}/{file_name}.json', 'x+'
                with open(diagrm_file, file_open_flag) as file:
                    file.write(json.dumps(pipeline_code))
                self.logger.info(f"Created new pipeline diagram: {file_name}", extra={
                    'execution_id': execution_id,
                    'action': 'diagram_create'
                })
                
        except Exception as e:
            self.logger.error(f"Failed to save pipeline diagram: {e}", extra={
                'execution_id': execution_id,
                'diagram_path': diagrm_path,
                'file_name': file_name,
                'exception': str(e)
            })
            raise
    
    def get_current_execution_id(self) -> Optional[str]:
        """
        Get the current execution ID for correlation.
        
        Returns:
            Current execution ID or None if not available
        """
        if self._pipeline_logger_config and self._pipeline_logger_config.get_context():
            return self._pipeline_logger_config.get_context().execution_id
        return None
    
    def add_correlation_context(self, **kwargs):
        """
        Add additional context to the current pipeline execution.
        
        Args:
            **kwargs: Additional context fields
        """
        if self._pipeline_logger_config:
            self._pipeline_logger_config.update_context(**kwargs)




    def update(self, file_path, file_name, data, context: RequestContext = None) -> Dict[str,str]:
        """
        This is the pipeline update and pipeline code
        """
        # Set up enhanced logging
        pipeline_name = self._get_pipeline_name_from_context(context) if context else file_name
        namespace = self._get_namespace_from_context(context) if context else "default"
        execution_id = self._setup_pipeline_logging(
            pipeline_name=pipeline_name,
            namespace=namespace,
            user_id=getattr(context, 'user', None),
            template_type="update"
        )
        
        try:
            ppline_file, _ = f'{file_path}/{file_name}', 'w+' 
            self.curr_file = ppline_file

            self._log_pipeline_event("info", f"Starting pipeline update: {pipeline_name}", 
                                   execution_id=execution_id, action="update", file_path=ppline_file)

            file_manager = FileVersionManager(file_path)
            file_manager.save_version(file_name)
            
            self._log_pipeline_event("info", "Pipeline version saved", 
                                   execution_id=execution_id, action="version_save")
     
            # Run pipeline generater above by passing the python file
            self._log_pipeline_event("info", f"Executing updated pipeline: {ppline_file}", 
                                   execution_id=execution_id, action="execute")
            
            result = subprocess.run(['python', ppline_file],check=True,
                                    capture_output=True,text=True)

            if result.returncode == 0:
                self._log_pipeline_event("info", "Pipeline update executed successfully", 
                                       execution_id=execution_id, return_code=result.returncode)
                if context is not None:
                    context.emit_ppsuccess()
            else:
                self._log_pipeline_event("warning", f"Pipeline update completed with non-zero return code: {result.returncode}", 
                                       execution_id=execution_id, return_code=result.returncode)

            print("Return Code:", result.returncode)
            print("Standard Output:", result.stdout)
            print("Standard Error:", result.stderr)

            return {
                'status': True,
                'message': 'Pipeline run terminated successfully'
            }
            
        except Exception as e:
            self._log_pipeline_event("error", f"Error during pipeline update: {e}", 
                                   execution_id=execution_id, exception=str(e))
            return {
                'status': False,
                'message': f'Pipeline update failed: {str(e)}'
            }
        finally:
            # Clean up logging resources
            self._cleanup_pipeline_logging()


    def update_ppline(self, file_path, file_name, data, context: RequestContext) -> Dict[str,str]:
        """
        Update the pipeline and file content It uses the create 
        method which will update since it exists already
        """
        self.update(file_path, file_name, data, context)


    @staticmethod
    def get_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/simple.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt


    @staticmethod
    def get_s3_no_auth_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/simple_s3_anon_login.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt

    @staticmethod
    def get_api_templete():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/api.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt
    
    
    @staticmethod
    def get_transform_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/simple_transform_field.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt
    
    
    @staticmethod
    def get_sql_db_template(tamplate_name = None):
        """
        This is template handling method
        """
        tplt = ''
        tplt_file = tamplate_name if tamplate_name != None else 'sql_db.txt'
        file_name = f'{template_dir}/{tplt_file}'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt    
    

    @staticmethod
    def get_mssql_db_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/sql_server.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt    


    @staticmethod
    def get_dlt_code_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/dlt_code.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt


    def save_instance(self, ppline_name, content):
        """
            This will save in the DB 
            the Pipeline created in the diagram (UI)
        """
        con = duckdb.connect("file.db")
        con.sql(
            "CREATE TABLE IF NOT EXISTS ppline_instances \
                (\
                    ppline_name, \
                    _content JSON, \
                    _timestamp TIMESTAMP\
                )\
            ")

        con.sql(
            f"INSERT INTO ppline_name, ppline_instances (_content) \
            VALUES \
            ('{ppline_name}','{json.dumps(content)}')")
        

    def revert_ppline(self):
        if(type(self.curr_file) == str):
            os.remove(self.curr_file)
        ...


    @staticmethod
    def get_template_from_existin_ppline(ppline_path):
        """
        This is template handling method that retrieves from exsiting pipeline
        """
        tplt = ''

        with open(f'{ppline_path}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt
    
    @staticmethod
    def create_new_pipline_version(file_name, file_path, data):
        file_manager = FileVersionManager(file_path)
        file_manager.save_version(f'{file_path}/{file_name}.py', data, 'altering transformation')
        new_file_name_version = file_manager.get_latest_version(f'{file_name}.py')

        return new_file_name_version
    

    processed_job = { 'start': {}, 'end': {} }
    @staticmethod
    def run_pipeline_job(file_path, namespace):
        # Set up enhanced logging for scheduled job
        pipeline_name = file_path.split('/')[-1] if '/' in file_path else file_path
        
        # Initialize logging system if needed
        try:
            processor = get_log_processor()
            if processor is None:
                storage = DuckDBLogStore()
                storage.initialize_tables()
                initialize_log_processor(storage)
        except Exception as e:
            print(f"Failed to initialize logging for job: {e}")
        
        # Set up pipeline logging configuration
        pipeline_logger_config = None
        execution_id = create_execution_id()
        
        try:
            pipeline_logger_config = PipelineLoggerConfig(
                pipeline_id=pipeline_name,
                log_level="INFO",
                namespace=namespace
            )
            
            pipeline_logger_config.configure_dlt_logging(
                execution_id=execution_id,
                pipeline_name=pipeline_name,
                template_type="scheduled_job"
            )
            
            job_logger = pipeline_logger_config.get_logger(f"job.{pipeline_name}")
            job_logger.info(f"Starting scheduled pipeline job: {pipeline_name}", extra={
                'execution_id': execution_id,
                'namespace': namespace,
                'job_type': 'scheduled'
            })
            
        except Exception as e:
            print(f"Failed to setup enhanced logging for job: {e}")
        
        ppline_file = f'{destinations_dir}/{file_path}.py'

        if not(os.path.exists(ppline_file)):
            ppline_file = f'{destinations_dir}/{file_path}|toschedule|.py'
            if not(os.path.exists(ppline_file)):
                ppline_file = f'{destinations_dir}/{file_path}|withmetadata|.py'

        db_root_path = destinations_dir.replace('pipeline','duckdb')
        # DB Lock in the pplication level
        if not(ppline_file.endswith('withmetadata|.py')\
              and ppline_file.endswith('withmetadata|.py')):
            DuckDBCache.set(f'{db_root_path}/{file_path}.duckdb','lock')

        socket_id = DuckdbUtil.get_socket_id(namespace)
        context = RequestContext(None, socket_id)
        job_execution_id = uuid.uuid4()

        try:
            if pipeline_logger_config:
                job_logger.info(f"Pipeline file resolved: {ppline_file}", extra={
                    'execution_id': execution_id,
                    'pipeline_file': ppline_file
                })
            
            DuckdbUtil.check_pipline_db(f'{db_root_path}/{file_path}.duckdb')
            print('####### WILL RUN JOB FOR '+file_path)

            if pipeline_logger_config:
                job_logger.info(f"Starting pipeline execution process", extra={
                    'execution_id': execution_id,
                    'database_path': f'{db_root_path}/{file_path}.duckdb'
                })

            # Run pipeline generater above by passing the python file
            result = subprocess.Popen(['python', ppline_file],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        text=True,
                                        bufsize=1)
            pipeline_exception = False

            while True:
                time.sleep(0.1)
                line = result.stdout.readline()
                if not line: 
                    break
                line = line.strip()
                
                # Log pipeline output with enhanced logging
                if pipeline_logger_config:
                    job_logger.debug(f"Job output: {line}", extra={
                        'execution_id': execution_id,
                        'output_line': line
                    })
                
                if (line == 'RUN_SUCCESSFULLY'):
                    if pipeline_logger_config:
                        job_logger.info("Pipeline job completed successfully", extra={
                            'execution_id': execution_id,
                            'status': 'success'
                        })
                    context.emit_ppsuccess()
                    pipeline_exception = False   
                
                else:
                    if(line.startswith('RUNTIME_ERROR:') or pipeline_exception == True):
                        pipeline_exception = True
                        error_message = line.replace('RUNTIME_ERROR:','')
                        if pipeline_logger_config:
                            job_logger.error(f"Runtime error in job: {error_message}", extra={
                                'execution_id': execution_id,
                                'error_type': 'runtime'
                            })
                        context.emit_ppline_job_trace(error_message, error=True)
                    else:
                        if(type(line) == str):
                            if(line.__contains__('Files/Bucket loaded')):
                                if(has_ppline_job('start',job_execution_id)):
                                    pass
                                if pipeline_logger_config:
                                    job_logger.info("Files/Bucket loaded successfully", extra={
                                        'execution_id': execution_id,
                                        'stage': 'data_loading'
                                    })
                        context.emit_ppline_job_trace(line)

                             
            #if result.returncode == 0 and context is not None and pipeline_exception == False:
            #    context.emit_ppsuccess()

            result.kill()
            
            if pipeline_exception == True:
                message = 'Runtime Pipeline error, check the logs for details'
                if pipeline_logger_config:
                    job_logger.error(f"Pipeline job failed: {message}", extra={
                        'execution_id': execution_id,
                        'final_status': 'failed'
                    })
                context.emit_ppline_job_trace(message, error=True)
            else:
                if(line.__contains__('Pipeline run terminated successfully')):
                    if(has_ppline_job('end',job_execution_id)):
                        pass
                if pipeline_logger_config:
                    job_logger.info("Pipeline job terminated successfully", extra={
                        'execution_id': execution_id,
                        'final_status': 'success'
                    })
                context.emit_ppline_job_trace('Pipeline run terminated successfully')
            
            error_messages, status = None, True
            if result.returncode != 0:
                err = str(result.stderr.read())
                print('THE ERROR IS ABOUT: ', err)
                if(err.__contains__('Could not set lock on file')):
                    if pipeline_logger_config:
                        job_logger.warning("Database lock conflict detected", extra={
                            'execution_id': execution_id,
                            'warning_type': 'lock_conflict'
                        })
                    pass

                error_messages = err.split('\n')
                if(str(error_messages).__contains__('[WARNING]')):
                    if pipeline_logger_config:
                        job_logger.warning(f"Job completed with warnings: {error_messages}", extra={
                            'execution_id': execution_id,
                            'warnings': error_messages
                        })
                    context.emit_ppline_trace(error_messages, warn=True)
                else:
                    message = '\n'.join(error_messages[1:])
                    if pipeline_logger_config:
                        job_logger.error(f"Job failed with errors: {message}", extra={
                            'execution_id': execution_id,
                            'error_messages': error_messages
                        })
                    context.emit_ppline_job_trace(message, error=True)
                    status = False

            if(status):
                context.emit_ppline_trace('PIPELINE COMPLETED SUCCESSFULLY')
                context.emit_ppsuccess()

            clear_job_transaction_id(job_execution_id)

            dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ppline_name = str(file_path).replace(f'{namespace}/','')
            DltPipeline.update_pipline_runtime(namespace,ppline_name,dt)

            if pipeline_logger_config:
                job_logger.info(f"Pipeline job execution completed", extra={
                    'execution_id': execution_id,
                    'completion_time': dt,
                    'final_status': 'success' if status else 'failed'
                })

            # DB Lock release in the pplication level
            DuckDBCache.remove(f'{db_root_path}/{file_path}.duckdb')
        
        except Exception as err:
            # DB Lock release in the pplication level
            DuckDBCache.remove(f'{db_root_path}/{file_path}.duckdb')
            message = f'Error while running job for {file_path.split('/')[1]} pipeline'
            
            if pipeline_logger_config:
                job_logger.error(f"Unexpected error in pipeline job: {err}", extra={
                    'execution_id': execution_id,
                    'exception': str(err),
                    'error_type': 'unexpected'
                })
            
            context.emit_ppline_job_trace(message,error=True)
            context.emit_ppline_job_trace(err.with_traceback,error=True)
        
        finally:
            # Clean up logging resources
            if pipeline_logger_config:
                try:
                    pipeline_logger_config.cleanup()
                except Exception as e:
                    print(f"Error cleaning up job logging: {e}")


    @staticmethod
    def update_pipline_runtime(namespace, ppline, time):
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = f"UPDATE ppline_schedule\
                    SET last_run='{time}'\
                    WHERE\
                        namespace='{namespace}'\
                        and ppline_name='{ppline}'"
        cnx.execute(query)


    @staticmethod
    def update_pipline_pause_status(namespace, ppline, is_paused):
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = f"UPDATE ppline_schedule\
                    SET is_paused='{is_paused}'\
                    WHERE\
                        namespace='{namespace}'\
                        and ppline_name='{ppline}'"
        cnx.execute(query)

        if is_paused != 'paused':
            from services.workspace.Workspace import Workspace
            Workspace.schedule_pipeline_job(namespace, ppline)
        else:
            tag_name = f'{namespace}_{ppline}'
            if schedule.get_jobs(tag_name):
                schedule.clear(tag_name)

            if schedule.get_jobs(f'{tag_name}-tracinglog'):
                schedule.clear(f'{tag_name}-tracinglog')
                        


    @staticmethod
    def get_pipline_runtime(namespace, ppline):
        time = datetime.now()
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = f"UPDATE ppline_schedule\
                    SET last_run='{time}'\
                    WHERE\
                        namespace='{namespace}'\
                        and ppline_name='{ppline}'"
        cnx.execute(query)


    @staticmethod
    def read_pipeline(file_path, namespace):

        try:
            code = ''
            with open(file_path, 'r') as file:
                code = file.read()
                pipeline_code = json.loads(code)

            node_list = pipeline_code['content']['Home']['data']
            database_obj = { id: node for id, node in node_list.items() if node['name'] == 'SqlDBComponent' }

            datasource_details = None
            if(len(database_obj.keys()) > 0):

                node = list(database_obj.values())[0]
                connection_name = node['data']['connectionName']
                datasource_details = SQLDatabase.get_tables_list(namespace, connection_name)
            
            if not(not(datasource_details)):
                del datasource_details['details']
                
            return pipeline_code, datasource_details
        
        except Exception as err:
            return {}, {}
        
    
    @staticmethod
    def get_sqldb_transformation_preview(namespace, dbengine, connection_name, script):
        result = run_transform_preview(namespace, dbengine, connection_name, script)
        return result        
    

    @staticmethod
    def get_file_data_transformation_preview(script):
        result = run_transform_preview(None, None, None, script)
        return result


def has_ppline_job(evt, job_transaction_id):

    if(DltPipeline.processed_job[evt].get(job_transaction_id,None) == None):
        DltPipeline.processed_job['end'][job_transaction_id] = True
        return False
    else:
        return True


def clear_job_transaction_id(job_transaction_id):
    if(job_transaction_id in DltPipeline.processed_job['start']):
        del DltPipeline.processed_job['start'][job_transaction_id]
    
    if(job_transaction_id in DltPipeline.processed_job['end']):
        del DltPipeline.processed_job['end'][job_transaction_id]


import ast

def check_unsafe_statements(code):
    """
    Raises ValueError if code contains disallowed statements or function calls
    """
    tree = ast.parse(code, mode="exec")
    valid_attrs = ['scan_csv','scan_parquet','scan_ndjson','with_columns','filter','collect','append','all','contains','lit','limit']

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ValueError("Import statements are not allowed")

        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_CALLS:
                raise ValueError(f"Use of function '{node.func.id}' is not allowed")

            #if isinstance(node.func, ast.Attribute) and not(node.func.attr in valid_attrs):
            #    raise ValueError("Attribute calls are not allowed")


def check_invalid_code(code):
    """
    Raises ValueError if code contains disallowed statements or function calls
    """
    
    code_lines = str(code).split('\n')

    for line in code_lines:

        if FORBIDDEN_CALLS_REGEX.search(line) or FORBIDDEN_DUNDER_REGEX.search(line):
            raise RuntimeError('Invalid code provided which might cause security breach')

        line_of_code = line.strip()
        is_from_import = line_of_code.startswith('from ')\
              and line.strip().__contains__(' import ')
        
        is_import = line_of_code.strip().startswith('import ')

        if(is_from_import or is_import):
            if not(line_of_code in valid_imports):
                raise RuntimeError('Invalid code provided which might cause security breach')



from utils.SQLServerUtil import column_type_conversion
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy import inspect
import polars as pl

def run_transform_preview(namespace, dbengine, connection_name, script):

    try:
        check_unsafe_statements(script)        
    except Exception as err:
        return { 'error': True, 'result': { 'msg': str(err), 'code': None } }

    try:
        engine, inspector = None, None
        inner_env = { 'pl': pl }

        if(namespace != None):
            engine = SQLDatabase.get_connnection(namespace,dbengine,connection_name)
            inspector = inspect(engine)
        
            inner_env = {
                'engine': engine, 'pl': pl, 'inspector': inspector,
                'column_type_conversion': column_type_conversion,
            }

        compile(script, '<transformation_task>', 'exec')

    except NoInspectionAvailable as err:
        error = 'Error while trying to connect to database'
        print(f'{error}: ', str(err))
        return { 'error': True, 'result': { 'msg': error, 'code': None } }        

    except SyntaxError as err:
        print('Error while running pipeline transformation preview: ', err.text)
        return { 'error': True, 'result': { 'msg': 'Syntax error', 'code': err.text } }

    except Exception as err:
        print('Error while running pipeline transformation preview: ');
        return { 'error': True, 'result': str(err) }

    try:
        exec(script, {}, inner_env)
    except Exception as err:
        print('Error while running pipeline transformation preview: ')
        print(err)
        return { 'error': True, 'result': { 'msg': str(err), 'code': None } }

    return { 'error': False, 'result': inner_env['results'] if 'results' in inner_env else None }