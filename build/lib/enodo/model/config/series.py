import json

from . import ConfigModel
from enodo.jobs import JOB_TYPES


class SeriesConfigModel(ConfigModel):

    __slots__ = ('job_models', 'job_schedule', 'min_data_points', 'model_params')
    
    def __init__(self, job_models, job_schedule, min_data_points=None, model_params=None):
        """
        Create new Series Config
        :param job_models: dict of job(key) and model name(value)
        :param job_schedule: dict of job(key) and n_new_points(value)
        :param model_params: dict of model(key) and dict(value)
        :return:
        """

        if not isinstance(job_models, dict):
            raise Exception("Invalid series config")

        for job in job_models:
            if job not in JOB_TYPES:
                raise Exception("Invalid series config")

        if not isinstance(job_schedule, dict):
            raise Exception("Invalid series config")

        self.job_models = job_models
        self.job_schedule = job_schedule
        self.min_data_points = min_data_points
        self.model_params = model_params

    def get_model_for_job(self, job_type):
        if job_type not in self.job_models:
            return False
        
        return self.job_models.get(job_type)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        return {
            'job_models': self.job_models,
            'job_schedule': self.job_schedule,
            'min_data_points': self.min_data_points,
            'model_params': self.model_params
        }