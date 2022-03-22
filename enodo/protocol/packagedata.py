import json
import logging
from abc import ABC, abstractmethod
from enodo.model.config.base import ConfigModel


class EnodoJobDataModel():

    def __init__(self, **kwargs):
        self._dict_values = kwargs
        if not self.validate():
            raise Exception("invalid data for packaga data")

        # self.__dict__ = json.loads(self._raw_data)

    def validate(self):
        if self.required_fields is not None:
            for key in self.required_fields:
                if key not in self._dict_values.keys():
                    logging.info(f"Missing '{key}' in enodo "
                                 "job data model data")
                    return False
        return "model_type" in self._dict_values.keys() and \
            self.validate_data(self._dict_values)

    @property
    @abstractmethod
    def required_fields(self):
        """ return list of required fields """

    @abstractmethod
    def validate_data(self):
        """ validate data """
        return True

    def get(self, key):
        return self._dict_values.get(key)

    def _children_to_dict(self):
        r = {}
        for key, child in self._dict_values.items():
            if isinstance(child, ConfigModel):
                r[key] = child.to_dict()
            else:
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
    def validate_by_job_type(cls, json_data, job_type):
        data = json.loads(json_data)

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
            "series_config"
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
            "forecast_points"
        ]


class EnodoDetectAnomaliesJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "anomaly_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "flagged_anomaly_points"
        ]


class EnodoBaseAnalysisJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "base_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "successful",
            "trend_slope_value",
            "noise_value",
            "has_seasonality",
            "health_of_series"
        ]


class EnodoStaticRulesJobResponseDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "static_rules_response"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "failed_checks"
        ]

    def validate_data(self, data):
        if not isinstance(data['failed_checks'], list):
            return False

        for failed_check in data['failed_checks']:
            if not isinstance(failed_check, list):
                return False

        return True
