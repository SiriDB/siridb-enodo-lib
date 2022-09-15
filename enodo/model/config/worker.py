
class WorkerConfigModel(dict):
    def __init__(self, config: dict, job_type: list):
        # if len(supported_job_types) < 1:
        #     raise Exception("Invalid supported job types")

        super(WorkerConfigModel, self).__init__({
            "config": config,
            "job_type": job_type
        })

    @property
    def config(self):
        return self.get("config")

    @config.setter
    def config(self, value):
        self["config"] = value

    @property
    def job_type(self):
        return self.get("job_type")

    @job_type.setter
    def job_type(self, value):
        self["job_type"] = value
