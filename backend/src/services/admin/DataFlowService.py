from typing import List, Dict, Any
from connectors.db.mysql import get_mysql_connection

class DataFlowService:
    """
    Service responsible for creating and handling data access for operational data flow.
    """

    connection = get_mysql_connection('e2eadmin')

    @staticmethod
    def create_template_mapping_table() -> None:
        """
        Creates the operational_pipeline table if it does not already exist.
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS operational_pipeline (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_source INT,
            source_type VARCHAR(25) NOT NULL,
            itermidiate_nodes LONGTEXT,
            destination LONGTEXT NOT NULL,
            namespace VARCHAR(100) NOT NULL,
            schedule VARCHAR(100),
            operation_state VARCHAR(100),
            creation_date DATETIME,
            created_by VARCHAR(150)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        
        cursor = DataFlowService.connection.cursor()
        try:
            cursor.execute(create_table_query)
            DataFlowService.connection.commit()
            print('Created the template table')
        finally:
            cursor.close()


    @staticmethod
    def create_flow(flows: List[Dict[str, Any]], namespace) -> dict:

        if not flows: { 'error': None, 'affected_rows': 0 }
        DataFlowService.create_template_mapping_table()

        insert_query = "INSERT INTO operational_pipeline (id_source, source_type, destination, namespace) VALUES  (%s, %s, %s, %s)"
        
        data_to_insert = [
            (t.get("id_source"), t.get("source_type"), t.get("destination"), namespace) for t in flows
        ]
        
        cursor = DataFlowService.connection.cursor()
        error, affected_rows = None, None
        try:
            cursor.executemany(insert_query, data_to_insert)
            DataFlowService.connection.commit()
            affected_rows = cursor.rowcount
        except Exception as err:
            error = str(err)
        finally:
            cursor.close()
            
        return { 'error': error, 'affected_rows': affected_rows }
    
    
    @staticmethod
    def get_template_records(namespace, template_name = None) -> dict:

        query = "SELECT template_name, template_type, connection_name, query FROM template_mapping WHERE namespace = %s"
        params = [namespace]

        if(template_name):
            query = f'{query} AND template_name = %s'
            params.append(template_name)
                
        cursor = DataFlowService.connection.cursor()
        error, result = None, None

        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
        except Exception as err:
            error = str(err)
        finally:
            cursor.close()
            
        return { 'error': error, 'result': result }