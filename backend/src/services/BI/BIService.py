from utils.metastore.BI.DashboardConfig import DashboardConfig
from utils.metastore.meta_storage import MetaStore

class BIService:

    @staticmethod
    def save_dashboard(namespace = None, charts_list=None, dashboard_name = None, dashboard_id = None):
        return MetaStore.save_dashboard(namespace, charts_list, dashboard_name, dashboard_id)    
    
    
    @staticmethod
    def save_chart(namespace, config_details, context, chart_name, data_source, chart_id):
        return MetaStore.save_analytics_chart(namespace, config_details, context, chart_name, data_source, chart_id)    
    
    
    @staticmethod
    def get_chart_configs(namespace, chart_name = None):
        return MetaStore.chart_config_store().get_chart_configs(namespace, chart_name)  
    
      
    @staticmethod
    def get_dashboard_configs(namespace):
        return MetaStore.chart_config_store().get_dashboard_configs(namespace)