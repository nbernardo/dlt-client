from .TemplateNodeType import TemplateNodeType

class DuckDBOutput(TemplateNodeType):
    """
    Bucket type mapping class
    """

    def __init__(self, data: dict):
        """
        Initialize the instance
        """
        self.database = data['database']
        self.table_name = data['tableName']

    def run(self):
        """
        Run the initial steps
        """
        super().run()
        print(f'Inited DuckDB with : {self.database} and {self.table_name}')
        