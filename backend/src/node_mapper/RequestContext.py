class RequestContext:

    def __init__(self, ppline_name=None, socket_sid=None):
        self.ppline_name = ppline_name
        self.exceptions = []
        self.ppline_files_path = "/home/nakassony/dlt-project/backend/src"
        self.socket_sid = socket_sid

    def add_exception(self, type, error):
        """
        Register any likely error from pipeline phase
        """
        self.exceptions.append({type: error})
