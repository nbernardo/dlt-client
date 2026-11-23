import json
from os import getenv as env
from controller.RequestContext import RequestContext
import uuid
from datetime import datetime
import logging
import re
from services.email.SimpleAPIMailer import SimpleAPIMailer
from internationalization.email import labels
import asyncio

SUCCESS_RUN_MESSAGE = 'Pipeline run terminated successfully'
DW_WAIT_SEC = int(env('STAGE_TO_DW_WAIT_SEC'))
JOB_RETRY_COUNT = int(env('JOB_RETRY_COUNT')) - 1 if int(env('JOB_RETRY_COUNT')) > 0 else 1


class OperationPipeline:
    """ This is the class to create and handle operational pipelines """
    def __init__(self):
        self.curr_file = None
        self.logger = logging.getLogger(__name__)

    def send_fail_email(start_time, context: RequestContext):
        start_dt, end_dt = datetime.fromtimestamp(float(start_time)), datetime.now()
        intl = labels[env('APP_LANG')]
        pipeline_name = context.pipeline_metadata.stage_storage.split('_for_',1)[1]
        content = intl['PPLINE_FAIL_TXT'].replace('{pp_name}', pipeline_name).replace('{sdate}', str(start_dt)).replace('{tstamp}', str(end_dt))
        [exec_fail_sbj_pfx, exec_fail_sbj_sfx] = [intl['PPLINE_FAIL_EXEC_SBJ_PFX'], intl['PPLINE_FAIL_EXEC_SBJ_SFX']]

        subject = f'{exec_fail_sbj_pfx} ({pipeline_name}) {exec_fail_sbj_sfx}'
        SimpleAPIMailer.send_email(env('PPLINE_RESULT_EMAIL'), env('PPLINE_RESULT_EMAIL_RCVR'), content, subject)


    @staticmethod
    def run_pipeline_job_sync(ppline_path: str, namespace, lead_pipeline=None, exec_id=None, sched_time=None, user=None):
        
        param_list = { 'exp_backoff': 1, 'sched_time': sched_time, 'req_user': user }
        if(exec_id): param_list = { **param_list, 'exec_id': exec_id, 'manual_run': True }
        asyncio.run(OperationPipeline.run_pipeline_job(ppline_path, namespace, param_list=param_list))


    @staticmethod
    async def run_pipeline_job(ppline_path, namespace, param_list: dict = {'exp_backoff': 1, 'req_user': None}):
        from services.admin.DataFlowService import DataFlowService
        ppline_path = str(ppline_path).split('/')
        namespace = namespace or ppline_path[-2]
        
        pipeline_details = DataFlowService.get_data_flow(namespace, ppline_path[-1])
        from controller.pipeline import parse_node
        import ast

        context = RequestContext()
        context.pipeline_name = pipeline_details['result'][0][0]
        context.connections = json.loads(json.loads(pipeline_details['result'][0][1]))
        context.transaction_namespace = namespace
        context.user = namespace
        context.operational_metadata.transaction = True

        all_nodes_content = json.loads(pipeline_details['result'][0][2])
        regex1, rplce = r'(<[^>]+?)"([^>]+?)"', r"\1'\2'"
        # Escape HTML tag in the content
        node_list_content = re.sub(regex1, rplce, all_nodes_content)

        context.node_params = ast.literal_eval(node_list_content)

        parse_node(context.connections, context.node_params, {}, context, [])

        return pipeline_details


    @staticmethod
    def ui_fail_notify(context: RequestContext, err, exec_id):
        context.emit_error(error=err, exec_id = exec_id)


    @staticmethod
    def _retry(ppline_file_path, param_list, job_start_time, namespace, pipeline, err):
        return