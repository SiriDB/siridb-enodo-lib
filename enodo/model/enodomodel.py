class EnodoModel:
    __slots__ = ('name', 'model_arguments', 'supported_jobs')

    def __init__(self, name, model_arguments, supported_jobs=[]):
        """
        :param name:
        :param model_arguments:  in form of  {'name': ..., 'required': True, 'description': ''} 
        """
        self.name = name
        self.model_arguments = model_arguments

        self.supported_jobs = supported_jobs

    def support_job_type(self, job_type):
        return job_type in self.supported_jobs

    @classmethod
    def to_dict(cls, model):
        return {
            'name': model.name,
            'model_arguments': model.model_arguments,
            'supported_jobs': model.supported_jobs
        }

    @classmethod
    def from_dict(cls, model):
        return EnodoModel(**model)