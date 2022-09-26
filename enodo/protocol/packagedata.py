import json
import logging
from abc import abstractmethod
from typing import Optional
from uuid import uuid4


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
    def unserialize(cls, data):
        if isinstance(data, str):
            data = json.loads(data)
        model_type = data.get("model_type")

        if model_type == "forecast_response":
            return EnodoForecastJobResponseDataModel(**data)
        elif model_type == "anomaly_response":
            return EnodoDetectAnomaliesJobResponseDataModel(**data)
        elif model_type == "base_response":
            return EnodoBaseAnalysisJobResponseDataModel(**data)
        elif model_type == "static_rules_response":
            return EnodoStaticRulesJobResponseDataModel(**data)

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
        except Exception as _:
            return False

        return True


class EnodoRequestConfig(dict):

    def __init__(self,
                 config_name: str,
                 job_type: str,
                 max_n_points: Optional[int] = 100000,
                 module_params: Optional[dict] = {}):
        super().__init__({
            'config_name': config_name,
            'job_type': job_type,
            'max_n_points': max_n_points,
            'module_params': module_params
        })

    @property
    def config_name(self) -> str:
        return self['config_name']

    @property
    def job_type(self) -> str:
        return self['job_type']

    @property
    def max_n_points(self) -> int:
        return self['max_n_points']

    @property
    def module_params(self) -> dict:
        return self['module_params']


class EnodoRequest(dict):

    def __init__(self,
                 series_name: str,
                 request_type: str,
                 config: Optional[EnodoRequestConfig] = None,
                 hub_id: Optional[int] = None,
                 pool_id: Optional[int] = None,
                 worker_id: Optional[int] = None):
        super().__init__({
            'series_name': series_name,
            'request_id': str(uuid4()).replace("-", ""),
            'request_type': request_type,
            'config': config,
            'hub_id': hub_id,
            'pool_id': pool_id,
            'worker_id': worker_id
        })

    @property
    def series_name(self) -> str:
        return self['series_name']

    @property
    def request_id(self) -> str:
        return self['request_id']

    @property
    def request_type(self) -> str:
        return self['request_type']

    @property
    def config(self) -> EnodoRequestConfig:
        return self['config']


class EnodoRequestResponse(dict):

    def __init__(self, series_name, request_id, response):
        super().__init__({
            'series_name': series_name,
            'request_id': request_id,
            'response': response
        })

    @property
    def series_name(self):
        return self['series_name']

    @property
    def request_id(self):
        return self['request_id']

    @property
    def response(self):
        return self['response']


class EnodoForecastJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "forecast_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "data",
            "analyse_region"
        ]


class EnodoDetectAnomaliesJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "anomaly_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "data",
            "analyse_region"
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
            "data",
            "analyse_region"
        ]

    def validate_data(self, data):
        if not isinstance(data['data'], list):
            return False

        for failed_check in data['data']:
            if not isinstance(failed_check, list):
                return False

        return True
