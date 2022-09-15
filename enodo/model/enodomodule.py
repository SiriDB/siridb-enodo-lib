class EnodoModuleArgument(dict):

    def __init__(self, name, required, description, value_type=""):
        super(EnodoModuleArgument, self).__init__({
            "name": name,
            "required": required,
            "description": description,
            "value_type": value_type})

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
    def value_type(self):
        return self.get("value_type")


class EnodoModule(dict):
    def __init__(
            self, name, version, module_arguments, job_type,
            job_load_weight={}):
        """
        :param name:
        :param module_arguments:  EnodoModuleArgument
        """
        arguments_list = [EnodoModuleArgument(
            **ma) for ma in module_arguments]
        super(EnodoModule, self).__init__({
            "name": name,
            "version": version,
            "module_arguments": arguments_list,
            "job_type": job_type,
            "job_load_weight": job_load_weight
        })

    @property
    def name(self):
        return self.get("name")

    @property
    def version(self):
        return self.get("version")

    @property
    def module_arguments(self):
        return self.get("module_arguments")

    @property
    def job_type(self):
        return self.get("job_type")

    @property
    def job_load_weight(self):
        return self.get("job_load_weight")

    def support_job_type(self, job_type):
        return job_type in self.supported_jobs

    def conform_to_params(self, job_type, params):
        for arg in self.get('module_arguments'):
            if arg.required and job_type in arg.job_types \
                    and arg.name not in params:
                return False
        return True

    def get_job_load_weight(self, job_type):
        return self.job_load_weight.get(job_type)
