import asyncio
import logging
from typing import Callable, Optional
from enodo.net import (
    PROTO_REQ_WORKER_QUERY, PROTO_REQ_WORKER_REQUEST, PROTO_RES_HANDSHAKE_FAIL,
    PROTO_RES_HANDSHAKE_OK, PROTO_RES_WORKER_QUERY, BaseProtocol, Package,
    PROTO_RES_HEARTBEAT, PROTO_REQ_HANDSHAKE, PROTO_RES_WORKER_REQUEST)
from enodo.protocol.packagedata import EnodoQuery, EnodoRequest, EnodoRequestResponse
from enodo.worker.hub import ClientManager, HubClient


class WorkerProtocol(BaseProtocol):

    def __init__(self, worker,
                 connection_lost: Optional[Callable] = None):
        super().__init__()
        self._worker = worker
        self.set_connection_lost(connection_lost)  # may be None at this time

    def connection_lost(self, exc: Optional[Exception]):
        super().connection_lost(exc)
        if self._connection_lost:
            self._connection_lost()

    def set_connection_lost(self, connection_lost: Callable):
        self._connection_lost = connection_lost

    async def _wrapper(self, foo):
        return await self._handler_wrapper(foo)

    async def _on_handshake(self, pkg: Package):
        logging.debug("Hands shaked with hub")
        worker_config = pkg.data.get('worker_config')
        hub_id = pkg.data.get('hub_id')
        client=HubClient(hub_id, self.transport, worker_config)
        if worker_config['job_type_id'] != self._worker._job_type_id:
            logging.error("Hub connected with config "
                          "which has invalid job types set")
            client.send(None, PROTO_RES_HANDSHAKE_FAIL)
            return False
        ClientManager.add_client(client, hub_id)
        self._worker.settings = ClientManager.clients[hub_id].worker_config
        ClientManager.clients[hub_id].send(None, PROTO_RES_HANDSHAKE_OK)

    async def _on_heartbeat(self, pkg: Package):
        resp_pkg = Package.make(
            PROTO_RES_HEARTBEAT, data=None)
        self.transport.write(resp_pkg.to_bytes())

    async def _on_worker_query(self, pkg: Package):
        logging.debug("Received worker query")
        try:
            query = EnodoQuery(**pkg.data)
        except Exception as e:
            logging.error("Received invalid query data")
            logging.debug("Corresponding error: ", str(e))
        else:
            result = self._worker.get_query_result(query)
            resp_pkg = Package.make(
                PROTO_RES_WORKER_QUERY, data=result)
            self.transport.write(resp_pkg.to_bytes())


    async def _on_worker_request(self, pkg: Package):
        logging.debug("Received worker request")
        try:
            request = EnodoRequest(**pkg.data)
        except:
            logging.error("Received invalid request data")
        else:
            self._worker.add_request(request)

    async def _on_worker_request_response(self, pkg: Package):
        logging.debug("Response for requested job")
        try:
            response = EnodoRequestResponse(**pkg.data)
        except Exception:
            logging.warning("Received Invalid request response")
        else:
            await self._worker.handle_request_response(response)

    def _get_future(self, pkg: Package) -> asyncio.Future:
        future, task = self._requests.pop(pkg.pid, (None, None))
        if future is None:
            logging.error(
                f'got a response on pkg id {pkg.pid} but the original '
                'request has probably timed-out'
            )
            return
        task.cancel()
        return future

    def on_package_received(self, pkg: Package, _map={
        PROTO_REQ_WORKER_REQUEST: _on_worker_request,
        PROTO_REQ_HANDSHAKE: _on_handshake,
        PROTO_RES_HEARTBEAT: _on_heartbeat,
        PROTO_RES_WORKER_REQUEST: _on_worker_request_response,
        PROTO_REQ_WORKER_QUERY: _on_worker_query
    }):
        handle = _map.get(pkg.tp)
        pkg.read_data()

        if handle is None:
            logging.error(f'unhandled package type: {pkg.tp}')
        else:
            asyncio.ensure_future(handle(self, pkg))
