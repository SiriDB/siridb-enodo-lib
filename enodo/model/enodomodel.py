class EnodoModel:
    __slots__ = ('model_name', 'model_arguments')

    def __init__(self, name, model_arguments):
        """
        :param name:
        :param model_arguments:  in form of  {'key': True} Where key is argument name and
                                    value is wether or not it is mandatory
        """
        self.model_name = name
        self.model_arguments = model_arguments

    @classmethod
    def to_dict(cls, model):
        return {
            'model_name': model.model_name,
            'model_arguments': model.model_arguments
        }

    @classmethod
    def from_dict(cls, model):
        return EnodoModel(model.get('model_name'),
                          model.get('model_arguments'))


