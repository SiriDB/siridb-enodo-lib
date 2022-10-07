import uuid

from enodo.jobs import JOB_TYPE_IDS


class SeriesJobConfigModel(dict):

    def __init__(self, module, job_type_id, module_params, max_n_points=None,
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

        if job_type_id not in JOB_TYPE_IDS.values():
            raise Exception(
                "Invalid series job config, unknown job_type_id")

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
            "job_type_id": job_type_id,
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
    def job_type_id(self):
        return self.get("job_type_id")

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
            self, job_config, rid=None, min_data_points=None,
            realtime=False):
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

        data = {
            "job_config": _job_config_list,
            "min_data_points": min_data_points,
            "realtime": realtime
        }
        if rid is not None:
            data['rid'] = rid

        super(SeriesConfigModel, self).__init__(data)

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

    @property
    def rid(self):
        # For reference to template
        return self.get("rid")

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
