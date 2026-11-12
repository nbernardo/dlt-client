from mysql.connector.connection import MySQLConnection
from typing import List, Dict, Any
from connectors.db.mysql import get_mysql_connection

class MappingTemplateService:
    """
    Service responsible for handling MySQL database migrations 
    for template mapping and related data using mysql-connector-python.
    """

    connection = get_mysql_connection('e2eadmin')

    @staticmethod
    def create_template_mapping_table() -> None:
        """
        Creates the template_mapping table if it does not already exist.
        
        Args:
            connection: An active MySQLConnection object.
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS template_mapping (
            id INT AUTO_INCREMENT PRIMARY KEY,
            template_name LONGTEXT NOT NULL,
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
        
        cursor = MappingTemplateService.connection.cursor()
        try:
            cursor.execute(create_table_query)
            MappingTemplateService.connection.commit()
            print('Created the template table')
        finally:
            cursor.close()

    @staticmethod
    def create_template_records(templates: List[Dict[str, Any]], namespace) -> int:
        """
        Migrates a batch of template mapping records into the MySQL database.
        Args: templates: A list of dictionaries containing template attributes.
        """
        if not templates: { 'error': None, 'affected_rows': 0 }

        insert_query = """
        INSERT INTO template_mapping 
            (template_name, template_content, template_type, connection_name, version, namespace)
        VALUES  (%s, %s, %s, %s, %s, %s) """
        
        # Map the dictionary items to a tuple list for efficient bulk insertion
        data_to_insert = [
            (
                t.get("name"),
                t.get("template_content"),
                t.get("template_type"),
                t.get("connection"),
                1, namespace,
            )
            for t in templates
        ]
        
        cursor = MappingTemplateService.connection.cursor()
        error, affected_rows = None, None
        try:
            cursor.executemany(insert_query, data_to_insert)
            MappingTemplateService.connection.commit()
            affected_rows = cursor.rowcount
        except Exception as err:
            error = str(err)
        finally:
            cursor.close()
            
        return { 'error': error, 'affected_rows': affected_rows }