import json
import logging
from abc import ABC, abstractmethod
from enodo.jobs import JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_TYPE_FORECAST_SERIES, JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES

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
                    logging.info(f"Missing '{key}' in enodo job data model data")
                    return False
        return "model_type" in self._dict_values.keys()

    @property
    @abstractmethod
    def required_fields(self):
        """ return the sound the animal makes """

    def get(self, key):
        return self._dict_values.get(key)

    def serialize(self):
        return json.dumps(self._dict_values)

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
        elif model_type == "forecast_response":
            return EnodoForecastJobResponseDataModel(**data)
        elif model_type == "job_request":
            return EnodoJobRequestDataModel(**data)
        

        return None


class EnodoJobRequestDataModel(EnodoJobDataModel):

    def __init__(self, **kwargs):
        kwargs['model_type'] = "job_request"
        super().__init__(**kwargs)

    @property
    def required_fields(self):
        return [
            "job_id",
            "job_type",
            "series_name",
            "series_config",
            "global_series_config"
        ]


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