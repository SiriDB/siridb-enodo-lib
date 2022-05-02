import json
import logging
from abc import abstractmethod


class EnodoJobDataModel():

    def __init__(self, **kwargs):
        self._dict_values = kwargs
        if not self.validate():
            raise Exception("invalid data for packaga data")

    def validate(self):
        if self.required_fields is not None:
            if "errors" in self._dict_values.keys():
                return True
            for key in self.required_fields:
                if key not in self._dict_values.keys():
                    logging.error(f"Missing '{key}' in enodo "
                                  "job data model data")
                    return False
        return "model_type" in self._dict_values.keys() and \
            self.validate_data(self._dict_values)

    @property
    @abstractmethod
    def required_fields(self):
        """ return list of required fields """

    def validate_data(self, data):
        """ validate data """
        return True

    def get(self, key):
        return self._dict_values.get(key)

    def _children_to_dict(self):
        r = {}
        for key, child in self._dict_values.items():
            r[key] = child

        return r

    def serialize(self):
        return json.dumps(self._children_to_dict())

    @classmethod
    def unserialize(cls, json_data):
        data = json.loads(json_data)
        model_type = data.get("model_type")

        if model_type == "forecast_response":
            return EnodoForecastJobResponseDataModel(**data)
        elif model_type == "anomaly_response":
            return EnodoDetectAnomaliesJobResponseDataModel(**data)
        elif model_type == "base_response":
            return EnodoBaseAnalysisJobResponseDataModel(**data)
        elif model_type == "static_rules_response":
            return EnodoStaticRulesJobResponseDataModel(**data)
        elif model_type == "job_request":
            return EnodoJobRequestDataModel(**data)

        return None

    @classmethod
    def validate_by_job_type(cls, data, job_type):

        try:
            if job_type == "job_forecast":
                return EnodoForecastJobResponseDataModel(**data)
            elif job_type == "job_anomaly_detect":
                return EnodoDetectAnomaliesJobResponseDataModel(**data)
            elif job_type == "job_base_analysis":
                return EnodoBaseAnalysisJobResponseDataModel(**data)
            elif job_type == "job_static_rules":
                return EnodoStaticRulesJobResponseDataModel(**data)
            elif job_type == "job_request":
                return EnodoJobRequestDataModel(**data)
        except Exception as _:
            return False

        return True


class EnodoJobRequestDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "job_request"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "job_id",
            "series_name",
            "job_config",
            "series_config",
            "series_state"
        ]

    # TODO add optional fields for explicity
    # Optional: required_job_config


class EnodoForecastJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "forecast_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "data"
        ]


class EnodoDetectAnomaliesJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "anomaly_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "data"
        ]


class EnodoBaseAnalysisJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "base_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "characteristics",
            "health"
        ]


class EnodoStaticRulesJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "static_rules_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "data"
        ]

    def validate_data(self, data):
        if not isinstance(data['data'], list):
            return False

        for failed_check in data['data']:
            if not isinstance(failed_check, list):
                return False

        return True
