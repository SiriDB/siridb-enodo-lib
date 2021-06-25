import json

from . import ConfigModel
from enodo.jobs import JOB_TYPES


class SeriesJobConfigModel(ConfigModel):
    __slots__ = ('activated', 'model', 'job_schedule', 'model_params')

    def __init__(self, model, job_schedule, model_params, activated=True):

        if not isinstance(activated, bool):
            raise Exception("Invalid series job config")

        if not isinstance(model, str):
            raise Exception("Invalid series job config")

        if not isinstance(job_schedule, int):
            raise Exception("Invalid series job config")

        if not isinstance(model_params, dict):
            raise Exception("Invalid series job config")

        self.activated = activated
        self.model = model
        self.job_schedule = job_schedule
        self.model_params = model_params

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        return {
            'activated': self.activated,
            'model': self.model,
            'job_schedule': self.job_schedule,
            'model_params': self.model_params
        }

class SeriesConfigModel(ConfigModel):

    __slots__ = ('job_models', 'min_data_points', 'realtime')
    
    def __init__(self, job_config, min_data_points=None, realtime=False):
        """
        Create new Series Config
        :param job_config: dict of job(key) and config(value)
        :param min_data_points: int value of min points before it will be analysed or used in a job
        :param realtime: boolean if series should be analysed in realtime with datapoint updates
        :return:
        """

        if not isinstance(job_config, dict):
            raise Exception("Invalid series config")

        self.job_config = {}
        for job in job_config:
            jmc = SeriesJobConfigModel.from_dict(job_config[job])
            self.job_config[job] = jmc

        if not isinstance(min_data_points, int):
            raise Exception("Invalid series config")

        if not isinstance(realtime, bool):
            raise Exception("Invalid series config")

        self.min_data_points = min_data_points
        self.realtime = realtime

    def get_config_for_job(self, job_type):
        if job_type not in self.job_config:
            return False
        
        return self.job_config.get(job_type)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        return {
            'job_config': {key:value.to_dict() for (key,value) in self.job_config.items()},
            'min_data_points': self.min_data_points,
            'realtime': self.realtime
        }