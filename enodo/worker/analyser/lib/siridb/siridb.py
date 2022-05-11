import logging

from siridb.connector import SiriDBClient
from siridb.connector.lib.exceptions import QueryError, InsertError, \
    ServerError, PoolError, AuthenticationError, UserAuthError


def config_equals(a, b):
    if a.get('username') != b.get('username'):
        return False
    if a.get('password') != b.get('password'):
        return False
    if a.get('dbname') != b.get('dbname'):
        return False
    if a.get('hostlist')[0] != b.get('hostlist')[0]:
        return False

    return True


class SiriDB:
    siri = None
    siridb_connected = False
    siridb_status = ""

    def __init__(self, siridb_user, siridb_password, siridb_db,
                 siridb_host, siridb_port):
        self.siri = SiriDBClient(
            username=siridb_user,
            password=siridb_password,
            dbname=siridb_db,
            # Multiple connections are supported
            hostlist=[(siridb_host, siridb_port)],
            keepalive=True)

    # @classmethod
    async def query_series_datapoint_count(self, series_name):
        await self.siri.connect()
        count = None
        try:
            result = await self.siri.query(
                f'select count() from "{series_name}"')
        except (QueryError, InsertError, ServerError,
                PoolError, AuthenticationError, UserAuthError) as e:
            logging.error('Connection problem with SiriDB server')
        else:
            count = result.get(series_name, [])[0][1]
        self.siri.close()
        return count

    # @classmethod
    async def query_series_data(self, series_name, selector="*",
                                since=None):

        query = f'select {selector} from "{series_name}"'
        if since is not None:
            query = (f'select {selector} from "{series_name}" '
                     f'after now - {since}m')
        await self.siri.connect()
        result = None
        try:
            result = await self.siri.query(query)
        except (QueryError, InsertError, ServerError, PoolError,
                AuthenticationError, UserAuthError) as e:
            logging.error('Connection problem with SiriDB server')
        self.siri.close()
        return result

    async def run_query(self, query):
        await self.siri.connect()
        result = None
        try:
            result = await self.siri.query(query)
        except (QueryError, InsertError, ServerError, PoolError,
                AuthenticationError, UserAuthError) as e:
            logging.error('Connection problem with SiriDB server')
        self.siri.close()
        return result

    async def test_connection(self):
        try:
            await self.siri.connect()
        except Exception:
            return "Cannot connect", False

        try:
            await self.siri.query(f'show dbname')
        except (QueryError, InsertError, ServerError, PoolError,
                AuthenticationError, UserAuthError) as e:
            return repr(e), False
        else:
            return "", True
