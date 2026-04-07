from utils.metastore.BI.ChartConfig import ChartConfig
from utils.metastore.meta_storage import MetaStore

class BIService:

    @staticmethod
    def save_config(namespace, config_details, context, chart_name):
        return MetaStore.save_analytics_chart(namespace, config_details, context, chart_name)    
    
    
    @staticmethod
    def get_configs(namespace, chart_name = None):
        return MetaStore.chart_config_store().get_configs(namespace, chart_name)