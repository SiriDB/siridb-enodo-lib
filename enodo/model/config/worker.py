import json

from . import ConfigModel

WORKER_MODE_GLOBAL = "global"
WORKER_MODE_DEDICATED_JOB_TYPE = "dedicated_job_type"
WORKER_MODE_DEDICATED_SERIES = "dedicated_series"
WORKER_MODES = [WORKER_MODE_GLOBAL, WORKER_MODE_DEDICATED_JOB_TYPE, WORKER_MODE_DEDICATED_SERIES]

class WorkerConfigModel(ConfigModel):

    __slots__ = ('mode', 'job_type', 'series')
    
    def __init__(self, worker_mode, dedicated_job_type=None, dedicated_series_name=None):
        if worker_mode not in WORKER_MODES:
            raise Exception("Invalid worker mode")

        if worker_mode == WORKER_MODE_DEDICATED_JOB_TYPE and dedicated_job_type is None:
            raise Exception("Invalid worker config")

        if worker_mode == WORKER_MODE_DEDICATED_SERIES and dedicated_series_name is None:
            raise Exception("Invalid worker config")

        self.mode = worker_mode
        self.job_type = dedicated_job_type
        self.series = dedicated_series_name

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        return {
            'worker_mode': self.mode,
            'dedicated_job_type': self.job_type,
            'dedicated_series_name': self.series
        }