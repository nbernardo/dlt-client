class PaginateParam:
    start_param = None
    end_param = None
    batch_size: None
    start_record = 0

    def __init__(self, start_param, end_param, batch_size, start_record = 0):
        self.start_param = start_param
        self.end_param = end_param
        self.batch_size = batch_size
        self.start_record = start_record