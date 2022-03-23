class EnodoModel:
    __slots__ = ('name', 'model_arguments', 'supported_jobs',
                 'job_load_weight')

    def __init__(
            self, name, model_arguments, supported_jobs=[],
            job_load_weight={}):
        """
        :param name:
        :param model_arguments:  in form of
                {'name': ..., 'required': True, 'description': ''}
        """
        self.name = name
        self.model_arguments = model_arguments

        self.supported_jobs = supported_jobs
        # key/value. per job score of 1-10 float value
        self.job_load_weight = job_load_weight

    def support_job_type(self, job_type):
        return job_type in self.supported_jobs

    def get_job_load_weight(self, job_type):
        return self.job_load_weight.get(job_type)

    @classmethod
    def to_dict(cls, model):
        return {
            'name': model.name,
            'model_arguments': model.model_arguments,
            'supported_jobs': model.supported_jobs,
            'job_load_weight': model.job_load_weight
        }

    @classmethod
    def from_dict(cls, model):
        return EnodoModel(**model)
