import os
from configparser import RawConfigParser, _UNSET, SectionProxy, ConfigParser

EMPTY_CONFIG_FILE = {
    'worker': {
        'hostname': '',
        'port': '9103',
        'heartbeat_interval': '25',
        'max_job_duration': '120'
    },
    'siridb_data': {
        'host': '',
        'port': '',
        'user': '',
        'password': '',
        'database': '',
    },
    'siridb_output': {
        'host': '',
        'port': '',
        'user': '',
        'password': '',
        'database': '',
    }
}


def create_standard_config_file(path):
    _config = ConfigParser()

    for section in EMPTY_CONFIG_FILE:
        _config.add_section(section)
        for option in EMPTY_CONFIG_FILE[section]:
            _config.set(section, option,
                        EMPTY_CONFIG_FILE[section][option])

    with open(path, "w") as fh:
        _config.write(fh)


class EnodoConfigParser:

    # def __getitem__(self, key):
    #     if key != self.default_section and not self.has_section(key):
    #         return SectionProxy(self, key)
    #     return self._proxies[key]

    # def has_option(self, section, option):
    #     return True

    def get(
            self, option):
        """Edited default get func from RawConfigParser
        """
        env_value = os.getenv(
            f"ENODO_{option.upper()}")
        if env_value is None:
            raise Exception(
                f'Invalid config, missing environment variable '
                f'"ENODO_{option.upper()}"')
        return env_value
