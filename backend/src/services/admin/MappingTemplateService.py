from typing import List, Dict, Any
from connectors.db.mysql import get_mysql_connection

class MappingTemplateService:
    """
    Service responsible for Template mapping handling considering both migration Data access.
    """

    @staticmethod
    def create_template_mapping_table() -> None:
        """
        Creates the template_mapping table if it does not already exist.
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS template_mapping (
            id INT AUTO_INCREMENT PRIMARY KEY,
            template_name LONGTEXT NOT NULL,
            query LONGTEXT NOT NULL,
            yml_content LONGTEXT NOT NULL,
            template_content LONGTEXT NOT NULL,
            template_type VARCHAR(100) NOT NULL,
            connection_name VARCHAR(100) NOT NULL,
            namespace VARCHAR(100) NOT NULL,
            version VARCHAR(50) NOT NULL,
            creation_date DATETIME,
            created_by VARCHAR(150),
            INDEX idx_template_type (template_type),
            INDEX idx_version (version)
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
    def create_template_records(templates: List[Dict[str, Any]], namespace) -> dict:

        if not templates: { 'error': None, 'affected_rows': 0 }

        insert_query = """
        INSERT INTO template_mapping 
            (template_name, template_content, template_type, connection_name, version, namespace, query, yml_content)
        VALUES  (%s, %s, %s, %s, %s, %s, %s, %s) """
        
        data_to_insert = [
            (
                t.get("name"),
                t.get("template_content"),
                t.get("template_type"),
                t.get("connection"),
                1, namespace,
                t.get("query"),
                t.get("yml_content"),
            )
            for t in templates
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
    def get_template_records(namespace, template_name = None) -> dict:

        query = "SELECT template_name, template_type, connection_name, query, id, template_content FROM template_mapping WHERE namespace = %s"
        params = [namespace]

        if(template_name):
            query = f'{query} AND template_name = %s'
            params.append(template_name)

        conn = get_mysql_connection()    
        cursor = conn.cursor()
        error, result = None, None

        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
        except Exception as err:
            error = str(err)
        finally:
            cursor.close()
            conn.close()
            
        return { 'error': error, 'result': result }