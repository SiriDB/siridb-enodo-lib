import uuid

from enodo.jobs import (
    JOB_TYPES,
    JOB_STATUS_NONE,
)


class SeriesJobConfigModel(dict):

    def __init__(self, module, job_type, job_schedule_type,
                 job_schedule, module_params, max_n_points=None,
                 activated=True, config_name=None, silenced=False,
                 requires_job=None):

        if not isinstance(activated, bool):
            raise Exception(
                "Invalid series job config, activated property must be a bool")

        if not isinstance(module, str):
            raise Exception(
                "Invalid series job config, module property must be a string")

        if len(module.split("@")) != 2:
            raise Exception(
                "Invalid series job config, module property must have format "
                "<module_name>@<module_version>")

        if job_type not in JOB_TYPES:
            raise Exception(
                "Invalid series job config, unknown job_type")

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

        if not isinstance(module_params, dict):
            raise Exception(
                "Invalid series job config, "
                "module_params property must be a dict")

        if max_n_points is not None and not isinstance(
                max_n_points, int):
            raise Exception(
                "Invalid series job config, "
                "max_n_points property must be an integer")

        if not isinstance(silenced, bool):
            raise Exception(
                "Invalid series job config, "
                "silenced property must be a bool")

        if config_name is None:
            config_name = str(uuid.uuid4())
        elif " " in config_name:
            raise Exception(
                "Invalid series job config, "
                "config_name must not contains any spaces")

        super(SeriesJobConfigModel, self).__init__({
            "activated": activated,
            "module": module,
            "job_type": job_type,
            "job_schedule_type": job_schedule_type,
            "job_schedule": job_schedule,
            "max_n_points": max_n_points,
            "module_params": module_params,
            "config_name": config_name,
            "silenced": silenced,
            "requires_job": requires_job})

    @property
    def activated(self):
        return self.get("activated")

    @property
    def module(self):
        return self.get("module").split("@")[0]

    @property
    def module_version(self):
        return self.get("module").split("@")[1]

    @property
    def job_type(self):
        return self.get("job_type")

    @property
    def job_schedule_type(self):
        return self.get("job_schedule_type")

    @property
    def job_schedule(self):
        return self.get("job_schedule")

    @property
    def max_n_points(self):
        return self.get("max_n_points")

    @property
    def module_params(self):
        return self.get("module_params")

    @property
    def config_name(self):
        return self.get("config_name")

    @property
    def silenced(self):
        return self.get("silenced")

    @property
    def requires_job(self):
        return self.get("requires_job")


class SeriesConfigModel(dict):

    def __init__(
            self, job_config, min_data_points=None, realtime=False):
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

        _job_config_list = []
        for job in job_config:
            jmc = SeriesJobConfigModel(**job)
            _job_config_list.append(jmc)

        job_config_names = [job.config_name for job in _job_config_list]
        if len(job_config_names) != len(list(set(job_config_names))):
            raise Exception(
                "Invalid series config, dupplicate job config name")

        if not isinstance(min_data_points, int):
            raise Exception(
                "Invalid series config, "
                "min_data_points property must be an integer")

        if not isinstance(realtime, bool):
            raise Exception(
                "Invalid series config, realtime property must be a bool")

        super(SeriesConfigModel, self).__init__({
            "job_config": _job_config_list,
            "min_data_points": min_data_points,
            "realtime": realtime})

    @property
    def job_config(self):
        _job_config = {}
        for jmc in self['job_config']:
            _job_config[jmc.config_name] = jmc
        return _job_config

    @property
    def min_data_points(self):
        return self.get("min_data_points")

    @property
    def realtime(self):
        return self.get("realtime")

    def get_config_for_job_type(self, job_type, first_only=True):
        r = []
        for job in self['job_config']:
            if job.job_type == job_type:
                r.append(job)

        if first_only:
            return r[0] if len(r) > 0 else None
        return r

    def get_config_for_job(self, job_config_name):
        return self.job_config.get(job_config_name)

    def add_config_for_job(self, job_config: dict):
        jc = SeriesJobConfigModel(**job_config)
        if self.job_config.get(jc.config_name) is not None:
            raise Exception(
                "Cannot add job config. There already exists a "
                "job config with this name")

        if jc.requires_job is not None and \
                self.job_config.get(jc.requires_job) is None:
            raise Exception(
                "Cannot add job config. "
                "The job requires a job which does not exist")

        self['job_config'].append(jc)

    def remove_config_for_job(self, job_config_name: str):
        job_config_to_remove = self.job_config.get(job_config_name)
        if job_config_to_remove is None:
            return False
        for job in self['job_config']:
            if job.requires_job == job_config_to_remove.config_name:
                raise Exception(
                    "Cannot remove job config because another "
                    "job requires the this job")
        filtered_jcs = [
            job for job in self['job_config']
            if job.config_name != job_config_name]
        self['job_config'] = filtered_jcs
        return True


class JobSchedule(dict):

    def __init__(self, schedule=None):

        if schedule is None:
            schedule = {}
        else:
            if not isinstance(schedule, dict):
                raise Exception("Invalid series job schedule")
            for job_config_name, schedule_item in schedule.items():
                if "value" not in schedule_item or "type" not in schedule_item:
                    raise Exception("Invalid series job schedule")
        super(JobSchedule, self).__init__(schedule)

    def get_job_schedule(self, job_config_name):
        return self.get(job_config_name, None)

    def set_job_schedule(self, job_config_name, value):
        if job_config_name not in self:
            self[job_config_name] = {}

        self[job_config_name] = value

    def remove_job_schedule(self, job_config_name):
        if job_config_name in self:
            del self[job_config_name]


class JobStatuses(dict):

    def __init__(self, statuses=None):

        if statuses is None:
            statuses = {}
        else:
            if not isinstance(statuses, dict):
                raise Exception("Invalid series job statuses")

        super(JobStatuses, self).__init__(statuses)

    def get_job_status(self, job_config_name):
        return self.get(job_config_name, JOB_STATUS_NONE)

    def set_job_status(self, job_config_name, value):
        self[job_config_name] = value

    def remove_job_status(self, job_config_name):
        if job_config_name in self:
            del self[job_config_name]


class JobCheckStatuses(dict):

    def __init__(self, statuses=None):

        if statuses is None:
            statuses = {}
        else:
            if not isinstance(statuses, dict):
                raise Exception("Invalid series job check statuses")

        super(JobCheckStatuses, self).__init__(statuses)

    def get_job_check_status(self, job_config_name):
        return self.get(job_config_name, "")

    def set_job_check_status(self, job_config_name, status_mesage):
        self[job_config_name] = status_mesage

    def remove_job_check_status(self, job_config_name):
        if job_config_name in self:
            del self[job_config_name]


class SeriesState(dict):

    def __init__(
            self,
            datapoint_count=None,
            health=None,
            interval=None,
            job_schedule=None,
            job_check_statuses=None,
            job_statuses=None):

        job_schedule = JobSchedule(job_schedule)
        job_statuses = JobStatuses(job_statuses)
        job_check_statuses = JobCheckStatuses(job_check_statuses)

        super(SeriesState, self).__init__({
            "datapoint_count": datapoint_count,
            "health": health,
            "interval": interval,
            "job_schedule": job_schedule,
            "job_statuses": job_statuses,
            "job_check_statuses": job_check_statuses
        })

    @property
    def datapoint_count(self):
        return self.get("datapoint_count")

    @datapoint_count.setter
    def datapoint_count(self, value):
        self["datapoint_count"] = value

    @property
    def health(self):
        return self.get("health")

    @health.setter
    def health(self, value):
        self["health"] = value

    @property
    def interval(self):
        return self.get("interval")

    @interval.setter
    def interval(self, value):
        self["interval"] = value

    @property
    def job_schedule(self):
        return self.get("job_schedule")

    @job_schedule.setter
    def job_schedule(self, value):
        self["job_schedule"] = value

    @property
    def job_statuses(self):
        return self.get("job_statuses")

    @job_statuses.setter
    def job_statuses(self, value):
        self["job_statuses"] = value

    @property
    def job_check_statuses(self):
        return self.get("job_check_statuses")

    @job_check_statuses.setter
    def job_check_statuses(self, value):
        self["job_check_statuses"] = value

    def get_job_status(self, job_config_name):
        return self['job_statuses'].get_job_status(job_config_name)

    def set_job_status(self, job_config_name, value):
        self['job_statuses'].set_job_status(job_config_name, value)

    def get_all_job_schedules(self):
        return self['job_schedule']

    def get_job_schedule(self, job_config_name):
        return self['job_schedule'].get_job_schedule(job_config_name)

    def set_job_schedule(self, job_config_name, value):
        self['job_schedule'].set_job_schedule(job_config_name, value)

    def get_job_check_status(self, job_config_name):
        return self['job_check_statuses'].get_job_check_status(
            job_config_name)

    def set_job_check_status(self, job_config_name, status_message):
        self['job_check_statuses'].set_job_check_status(
            job_config_name,
            status_message)

    def remove_job_state(self, job_config_name):
        self['job_statuses'].remove_job_status(job_config_name)
        self['job_check_statuses'].remove_job_check_status(
            job_config_name)
        self['job_schedule'].remove_job_schedule(job_config_name)
