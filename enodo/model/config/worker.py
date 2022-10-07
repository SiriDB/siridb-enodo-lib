
class WorkerConfigModel(dict):
    def __init__(self, config: dict, job_type_id: list):
        # if len(supported_job_types) < 1:
        #     raise Exception("Invalid supported job types")

        super(WorkerConfigModel, self).__init__({
            "config": config,
            "job_type_id": job_type_id
        })

    @property
    def config(self):
        return self.get("config")

    @config.setter
    def config(self, value):
        self["config"] = value

    @property
    def job_type_id(self):
        return self.get("job_type_id")

    @job_type_id.setter
    def job_type_id(self, value):
        self["job_type_id"] = value
