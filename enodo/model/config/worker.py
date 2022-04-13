WORKER_MODE_GLOBAL = "global"
WORKER_MODE_DEDICATED_JOB_TYPE = "dedicated_job_type"
WORKER_MODE_DEDICATED_SERIES = "dedicated_series"
WORKER_MODES = [
    WORKER_MODE_GLOBAL,
    WORKER_MODE_DEDICATED_JOB_TYPE,
    WORKER_MODE_DEDICATED_SERIES
]


class WorkerConfigModel(dict):
    def __init__(self, mode,
                 dedicated_job_type=None, dedicated_series_name=None):
        if mode not in WORKER_MODES:
            raise Exception("Invalid worker mode")

        if mode == WORKER_MODE_DEDICATED_JOB_TYPE and \
                dedicated_job_type is None:
            raise Exception("Invalid worker config")

        if mode == WORKER_MODE_DEDICATED_SERIES and \
                dedicated_series_name is None:
            raise Exception("Invalid worker config")

        super(WorkerConfigModel, self).__init__({
            "mode": mode,
            "dedicated_job_type": dedicated_job_type,
            'dedicated_series_name': dedicated_series_name
        })

    @property
    def mode(self):
        return self.get("mode")

    @mode.setter
    def mode(self, value):
        self["mode"] = value

    @property
    def dedicated_job_type(self):
        return self.get("dedicated_job_type")

    @dedicated_job_type.setter
    def dedicated_job_type(self, value):
        self["dedicated_job_type"] = value

    @property
    def dedicated_series_name(self):
        return self.get("dedicated_series_name")

    @dedicated_series_name.setter
    def dedicated_series_name(self, value):
        self["dedicated_series_name"] = value
