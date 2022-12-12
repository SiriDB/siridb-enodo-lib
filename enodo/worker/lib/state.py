class WorkerState(dict):

    def __init__(self, series_name):
        super().__init__({
            "series_name": series_name,
            "results": {},
            "errors": {}
        })

    def get_result(self, job_type):
        return self['results'].get(job_type)

    def set_result(self, job_type, result):
        self['results'][job_type] = result

    def get_errors(self, job_type):
        return self['errors'].get(job_type, [])

    def add_error(self, job_type, error):
        self['errors'][job_type] = error

    def clear_errors(self, job_type, error):
        self['errors'][job_type] = []