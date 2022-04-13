class EnodoModuleArgument(dict):

    def __init__(self, name, required, description, job_types):
        super(EnodoModuleArgument, self).__init__({
            "name": name,
            "required": required,
            "description": description,
            "job_types": job_types})

    @property
    def name(self):
        return self.get("name")

    @property
    def required(self):
        return self.get("required")

    @property
    def description(self):
        return self.get("description")

    @property
    def job_types(self):
        return self.get("job_types")


class EnodoModule(dict):
    def __init__(
            self, name, module_arguments, supported_jobs=[],
            job_load_weight={}):
        """
        :param name:
        :param module_arguments:  EnodoModuleArgument
        """
        super(EnodoModule, self).__init__({
            "name": name,
            "module_arguments": module_arguments,
            "supported_jobs": supported_jobs,
            "job_load_weight": job_load_weight
        })

    @property
    def name(self):
        return self.get("name")

    @property
    def module_arguments(self):
        return self.get("module_arguments")

    @property
    def supported_jobs(self):
        return self.get("supported_jobs")

    @property
    def job_load_weight(self):
        return self.get("job_load_weight")

    def support_job_type(self, job_type):
        return job_type in self.supported_jobs

    def get_job_load_weight(self, job_type):
        return self.job_load_weight.get(job_type)
