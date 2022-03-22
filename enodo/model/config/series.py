import json
import uuid

from . import ConfigModel
from enodo.jobs import (
    JOB_TYPE_BASE_SERIES_ANALYSIS,
    JOB_TYPE_STATIC_RULES,
    JOB_TYPES,
    JOB_TYPE_FORECAST_SERIES,
    JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES,
    JOB_STATUS_NONE,
)


class SeriesJobConfigModel(ConfigModel):
    __slots__ = (
        'activated',
        'model',
        'job_type',
        'job_schedule_type',
        'job_schedule',
        'model_params',
        'config_name',
        'silenced',
        'requires_job',
        'job_last_run',
    )

    def __init__(self, model, job_type, job_schedule_type, job_schedule,
                 model_params, activated=True, config_name=None,
                 silenced=False, requires_job=None, job_last_run=None):

        if not isinstance(activated, bool):
            raise Exception(
                "Invalid series job config, activated property must be a bool")

        if not isinstance(model, str):
            raise Exception(
                "Invalid series job config, model property must be a string")

        if job_type not in JOB_TYPES:
            raise Exception("Invalid series job config, unknown job_type")

        if not isinstance(job_schedule_type, str):
            raise Exception(
                "Invalid series job config, "
                "job_schedule_type property must be a string")

        if job_schedule_type not in ['N', 'TS']:
            raise Exception(
                "Invalid series job config, "
                "job_schedule_type property be one of: ['N', 'TS']")

        if not isinstance(job_schedule, int):
            raise Exception(
                "Invalid series job config, "
                "job_schedule property must be an integer")

        if not isinstance(model_params, dict):
            raise Exception(
                "Invalid series job config, "
                "model_params property must be a dict")

        if not isinstance(silenced, bool):
            raise Exception(
                "Invalid series job config, "
                "silenced property must be a bool")

        if config_name is None:
            config_name = str(uuid.uuid4())

        self.activated = activated
        self.model = model
        self.job_type = job_type
        self.job_schedule_type = job_schedule_type
        self.job_schedule = job_schedule
        self.model_params = model_params
        self.config_name = config_name
        self.silenced = silenced
        self.requires_job = requires_job

        self.job_last_run = None

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        return {
            'activated': self.activated,
            'model': self.model,
            'job_type': self.job_type,
            'job_schedule_type': self.job_schedule_type,
            'job_schedule': self.job_schedule,
            'model_params': self.model_params,
            'config_name': self.config_name,
            'silenced': self.silenced,
            'requires_job': self.requires_job,
            'job_last_run': self.job_last_run
        }


class SeriesConfigModel(ConfigModel):

    __slots__ = ('job_config', 'min_data_points', 'realtime')

    def __init__(self, job_config, min_data_points=None, realtime=False):
        """
        Create new Series Config
        :param job_config: dict of job(key) and config(value)
        :param min_data_points: int value of min points before it will be
            analysed or used in a job
        :param realtime: boolean if series should be analysed in realtime with
            datapoint updates
        :return:
        """

        if not isinstance(job_config, list):
            raise Exception(
                "Invalid series config, job_config property must be a list")

        self.job_config = {}
        for job in job_config:
            jmc = SeriesJobConfigModel.from_dict(job)
            self.job_config[jmc.config_name] = jmc

        if not isinstance(min_data_points, int):
            raise Exception(
                "Invalid series config, "
                "min_data_points property must be an integer")

        if not isinstance(realtime, bool):
            raise Exception(
                "Invalid series config, realtime property must be a bool")

        self.min_data_points = min_data_points
        self.realtime = realtime

    def get_config_for_job_type(self, job_type, first_only=True):
        r = []
        for job in self.job_config.values():
            if job.job_type == job_type:
                r.append(job)

        if first_only:
            return r[0] if len(r) > 0 else None
        return r

    def get_config_for_job(self, job_config_name):
        return self.job_config.get(job_config_name)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        return {
            'job_config': [
                value.to_dict() for value in self.job_config.values()],
            'min_data_points': self.min_data_points,
            'realtime': self.realtime
        }


class JobSchedule:

    def __init__(self, schedule=None):

        if schedule is None:
            schedule = {}
        else:
            if not isinstance(schedule, dict):
                raise Exception("Invalid series job schedule")
            for job_config_name, schedule in schedule.items():
                if "value" not in schedule or "type" not in schedule:
                    raise Exception("Invalid series job schedule")

        self.schedule = schedule

    def get_job_schedule(self, job_config_name):
        return self.schedule.get(job_config_name, None)

    def set_job_schedule(self, job_config_name, value):
        if job_config_name not in self.schedule:
            self.schedule[job_config_name] = {}

        self.schedule[job_config_name] = value

    @classmethod
    def from_dict(cls, data):
        return cls(data)

    def to_dict(self):
        return self.schedule


class JobStatuses:

    def __init__(self, statuses=None):

        if statuses is None:
            statuses = {}
        else:
            if not isinstance(statuses, dict):
                raise Exception("Invalid series job schedule")

        self.statuses = statuses

    def get_job_status(self, job_config_name):
        return self.statuses.get(job_config_name, JOB_STATUS_NONE)

    def set_job_status(self, job_config_name, value):
        self.statuses[job_config_name] = value

    @classmethod
    def from_dict(cls, data):
        return cls(data)

    def to_dict(self):
        return self.statuses


class SeriesState:

    __slots__ = ('datapoint_count', 'health', 'job_schedule', 'job_statuses')

    def __init__(
            self,
            datapoint_count=None,
            health=None,
            job_schedule=None,
            job_statuses=None):

        job_schedule = JobSchedule(job_schedule)
        job_statuses = JobStatuses(job_statuses)

        self.datapoint_count = datapoint_count
        self.health = health
        self.job_schedule = job_schedule
        self.job_statuses = job_statuses

    def get_job_status(self, job_config_name):
        return self.job_statuses.get_job_status(job_config_name)

    def set_job_status(self, job_config_name, value):
        return self.job_statuses.set_job_status(job_config_name, value)

    def get_all_job_schedules(self):
        return self.job_schedule.schedule

    def get_job_schedule(self, job_config_name):
        return self.job_schedule.get_job_schedule(job_config_name)

    def set_job_schedule(self, job_config_name, value):
        return self.job_schedule.set_job_schedule(job_config_name, value)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self):
        return {
            'datapoint_count': self.datapoint_count,
            'health': self.health,
            'job_schedule': self.job_schedule.to_dict(),
            'job_statuses': self.job_statuses.to_dict()
        }
