from time import time
from uuid import uuid4


ENODO_EVENT_ANOMALY_DETECTED = "event_anomaly_detected"
ENODO_EVENT_JOB_QUEUE_TOO_LONG = "job_queue_too_long"
ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE = "lost_client_without_goodbye"
ENODO_EVENT_WORKER_ERROR = "event_worker_error"
ENODO_EVENT_TYPES = [ENODO_EVENT_ANOMALY_DETECTED,
                     ENODO_EVENT_JOB_QUEUE_TOO_LONG,
                     ENODO_EVENT_LOST_CLIENT_WITHOUT_GOODBYE,
                     ENODO_EVENT_WORKER_ERROR]
ENODO_SERIES_RELATED_EVENT_TYPES = [
    ENODO_EVENT_ANOMALY_DETECTED, ENODO_EVENT_WORKER_ERROR]

ENODO_EVENT_OUTPUT_WEBHOOK = 1
ENODO_EVENT_OUTPUT_TYPES = [ENODO_EVENT_OUTPUT_WEBHOOK]

ENODO_EVENT_SEVERITY_INFO = "info"
ENODO_EVENT_SEVERITY_WARNING = "warning"
ENODO_EVENT_SEVERITY_ERROR = "error"
ENODO_EVENT_SEVERITY_LEVELS = [
    ENODO_EVENT_SEVERITY_INFO,
    ENODO_EVENT_SEVERITY_WARNING,
    ENODO_EVENT_SEVERITY_ERROR]


class EnodoEvent(dict):
    """
    EnodoEvent class. Holds data for an event (error/warning/etc)
    that occured. No state data is saved.
    """

    def __init__(self, title, message, event_type, series=None):
        if event_type not in ENODO_EVENT_TYPES:
            raise Exception()  # TODO Nice exception

        super().__init__({
            'title': title,
            'message': message,
            'event_types': event_type,
            'series': series,
            'ts': int(time()),
            'uuid': str(uuid4()).replace("-", "")
        })

    @property
    def title(self):
        return self['title']

    @property
    def message(self):
        return self['message']

    @property
    def event_types(self):
        return self['event_types']

    @property
    def series(self):
        return self['series']

    @property
    def ts(self):
        return self['ts']

    @property
    def uuid(self):
        return self['uuid']
