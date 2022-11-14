from typing import Any, Optional
from uuid import uuid4

from enodo.model.config.series import SeriesJobConfigModel


class EnodoRequestConfig(dict):

    def __init__(self,
                 config_name: str,
                 job_type_id: str,
                 max_n_points: Optional[int] = 100000,
                 module_params: Optional[dict] = {}):
        super().__init__({
            'config_name': config_name,
            'job_type_id': job_type_id,
            'max_n_points': max_n_points,
            'module_params': module_params
        })

    @property
    def config_name(self) -> str:
        return self['config_name']

    @property
    def job_type_id(self) -> str:
        return self['job_type_id']

    @property
    def max_n_points(self) -> int:
        return self['max_n_points']

    @property
    def module_params(self) -> dict:
        return self['module_params']

    @classmethod
    def from_job_config(cls, config: SeriesJobConfigModel):
        return cls(
            config.config_name,
            config.job_type_id,
            config.max_n_points,
            config.module_params
        )


REQUEST_TYPE_WORKER = 'worker'
REQUEST_TYPE_HUB = 'hub'
REQUEST_TYPE_EXTERNAL = 'external'

QUERY_SUBJECT_STATE = 'query_state'
QUERY_SUBJECT_STATS = 'query_stats'

class EnodoQuery(dict):

    def __init__(self,
                query_id: str,
                subject: str,
                series_name: Optional[str] = None,
                result: Optional[Any] = None):
        if subject not in [QUERY_SUBJECT_STATE, QUERY_SUBJECT_STATS]:
            raise Exception("Invalid subject for query")
        super().__init__({
            "query_id": query_id,
            "subject": subject,
            "series_name": series_name,
            "result": result
        })

    @property
    def query_id(self):
        return self['query_id']

    @property
    def subject(self):
        return self['subject']

    @property
    def series_name(self):
        return self['series_name']

    @property
    def result(self):
        return self['result']



class EnodoRequest(dict):

    def __init__(self,
                 series_name: str,
                 request_type: str,
                 request_id: Optional[str] = None,
                 response_output_id: Optional[Any] = None,
                 config: Optional[SeriesJobConfigModel] = None,
                 hub_id: Optional[int] = None,
                 pool_id: Optional[int] = None,
                 worker_id: Optional[int] = None,
                 meta: Optional[dict] = None):
        if request_type not in [
                REQUEST_TYPE_WORKER, REQUEST_TYPE_HUB, REQUEST_TYPE_EXTERNAL]:
            raise Exception(f"Invalid EnodoRequest type {request_type}")
        if config is not None and not isinstance(config, SeriesJobConfigModel):
            config = SeriesJobConfigModel(**config)
        super().__init__({
            'series_name': series_name,
            'request_id': request_id or str(uuid4()).replace("-", ""),
            'request_type': request_type,
            'response_output_id': response_output_id,
            'config': config,
            'hub_id': hub_id,
            'pool_id': pool_id,
            'worker_id': worker_id,
            'meta': meta
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
    def response_output_id(self) -> str:
        return self['response_output_id']

    @property
    def config(self) -> SeriesJobConfigModel:
        return self['config']

    @property
    def meta(self) -> dict:
        return self['meta']


class EnodoRequestResponse(dict):

    def __init__(self,
                 series_name,
                 request_id,
                 result,
                 request,
                 error=None,
                 meta=None):
        if not isinstance(request, EnodoRequest):
            request = EnodoRequest(**request)

        if meta is None:
            meta = {}
        super().__init__({
            'series_name': series_name,
            'request_id': request_id,
            'result': result,
            'request': request,
            'error': error,
            'meta': meta
        })

    @property
    def series_name(self):
        return self['series_name']

    @property
    def request_id(self):
        return self['request_id']

    @property
    def request(self) -> EnodoRequest:
        return self['request']

    @property
    def result(self):
        return self['result']

    @property
    def error(self):
        return self['error']

    @property
    def meta(self):
        return self['meta']
