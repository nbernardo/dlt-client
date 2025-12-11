from services.agents.DataQueryAIAssistent import DataQueryAIAssistent
from services.agents.PipelineAIAssistent import PipelineAIAssistent
from typing import List
import os 

agents_type_list = {
    'pipeline': 'pipeline-agent',
    'data': 'data-query-agent',
}

pipeline_agents_list: List[PipelineAIAssistent] = {}
dataquery_agents_list: List[DataQueryAIAssistent] = {}

def get_data_agent(user, namespace = None, namespace_folder = None) -> DataQueryAIAssistent:

    if(not(user in dataquery_agents_list)):
        if(not os.path.exists(namespace_folder)):
            return None
        dataquery_agents_list[user] = DataQueryAIAssistent(namespace_folder)
        dataquery_agents_list[user].namespace = namespace

    return dataquery_agents_list[user]
    


def get_pipeline_agent(namespace) -> PipelineAIAssistent:

    if(not(namespace in pipeline_agents_list)):

        try:
            pipeline_agents_list[namespace] = PipelineAIAssistent()
            pipeline_agents_list[namespace].namespace = namespace
        
        except Exception as err:
            return None
    
    return pipeline_agents_list[namespace]