class Logging:

    _queue = None

    @classmethod
    def _log(cls, level, msg):
        if cls._queue is None:
            return
        cls._queue.put({
            'level': level,
            'msg': msg
        })

    @classmethod
    def error(cls, msg):
        cls._log('error', msg)

    @classmethod
    def warning(cls, msg):
        cls._log('warning', msg)

    @classmethod
    def info(cls, msg):
        cls._log('info', msg)

    @classmethod
    def debug(cls, msg):
        cls._log('debug', msg)


logging = Logging  # Alias