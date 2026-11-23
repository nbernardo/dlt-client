from typing import List, Dict, Any
from connectors.db.mysql import get_mysql_connection

class DataFlowService:
    """
    Service responsible for creating and handling data access for operational data flow.
    """

    @staticmethod
    def create_template_mapping_table() -> None:
        """
        Creates the operational_pipeline table if it does not already exist.
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS operational_pipeline (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_source INT,
            pipeline VARCHAR(100) NOT NULL,
            namespace VARCHAR(100) NOT NULL,
            source_type VARCHAR(25) NOT NULL,
            itermidiate_nodes LONGTEXT,
            destination LONGTEXT NOT NULL,
            node_connections LONGTEXT NOT NULL,
            node_list LONGTEXT NOT NULL,
            schedule VARCHAR(100),
            operation_state VARCHAR(100),
            status VARCHAR(100),
            creation_date DATETIME,
            created_by VARCHAR(150)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        conn = get_mysql_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(create_table_query)
            print('Created the template table')
        finally:
            cursor.close()
            conn.close()


    @staticmethod
    def create_flow(flows: List[Dict[str, Any]], namespace) -> dict:

        if not flows: { 'error': None, 'affected_rows': 0 }
        DataFlowService.create_template_mapping_table()

        insert_query = "INSERT INTO operational_pipeline " \
        "(pipeline, id_source, source_type, destination, namespace, node_connections, node_list) VALUES  (%s, %s, %s, %s, %s, %s, %s)"
        
        data_to_insert = [
            (t.get("pipeline"), t.get("id_source"), t.get("source_type"), t.get("destination"), namespace, t.get('node_connections'), t.get('node_list')) 
            for t in flows
        ]
        
        conn = get_mysql_connection()
        cursor = conn.cursor()
        error, affected_rows = None, None
        try:
            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            affected_rows = cursor.rowcount
        except Exception as err:
            error = str(err)
        finally:
            cursor.close()
            conn.close()
            
        return { 'error': error, 'affected_rows': affected_rows }
    
    
    @staticmethod
    def get_data_flow(namespace, pipeline_name = None, status = None) -> dict:

        query = "SELECT pipeline, node_connections, node_list FROM operational_pipeline WHERE namespace = %s"
        params = [namespace]
                
        if(pipeline_name):
            query = f'{query} AND pipeline = %s'
            params.append(pipeline_name)

        if(status):
            query = f'{query} AND status = %s'
            params.append(status)
        else:
            query = f'{query} AND status <> "archived"'

        conn = get_mysql_connection()        
        cursor = conn.cursor()
        error, result = None, None

        try:
            cursor.execute(f'{query} {'LIMIT 1' if pipeline_name else ''}', params)
            result = cursor.fetchall()
        except Exception as err:
            error = str('Error while fetching operation pipeline: '+err)
            print(error)
        finally:
            cursor.close()
            conn.close()
            
        return { 'error': error, 'result': result }    
    

    @staticmethod
    def update_data_flow(namespace, status, pipeline_name = None) -> dict:

        query = "UPDATE operational_pipeline SET status = %s WHERE namespace = %s"
        params = [status, namespace]
                
        if(pipeline_name):
            query = f'{query} AND pipeline = %s'
            params.append(pipeline_name)

        conn = get_mysql_connection()        
        cursor = conn.cursor()
        error, result = None, None

        try:
            cursor.execute(query, params)
            result = cursor.rowcount
            conn.commit()
        except Exception as err:
            error = str('Error while updating operation pipeline: '+err)
            print(error)
        finally:
            cursor.close()
            conn.close()
            
        return { 'error': error, 'result': result }