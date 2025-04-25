class RequestContext:

    ppline_files_path = "/home/nakassony/dlt-project/backend/src"

    def __init__(self, ppline_name):
        self.ppline_name = ppline_name
        self.exceptions = []

    def add_exception(self, type, error):
        """
        Register any likely error from pipeline phase
        """
        self.exceptions.append({type: error})
