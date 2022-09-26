import asyncio
import json
import logging
from multiprocessing import Process, Queue as MQueue, pool
from queue import Queue
from random import randrange
import signal
from time import time
from enodo.model.config.worker import WorkerConfigModel
from enodo.model.enodoevent import ENODO_EVENT_WORKER_ERROR, EnodoEvent
from enodo.worker.analyser.analyser import start_analysing
from enodo.version import __version__
from enodo.worker.lib.config import EnodoConfigParser
from enodo.worker.lib.util.util import get_dt_to_midnight

import qpack
from enodo.protocol.package import (
    EVENT, WORKER_QUERY, WORKER_REQUEST, WORKER_REQUEST_RESULT, create_header,
    read_packet, HEARTBEAT, HANDSHAKE, HANDSHAKE_FAIL, UNKNOWN_CLIENT,
    CLIENT_SHUTDOWN, HANDSHAKE_OK, WORKER_QUERY_RESULT)
from enodo.protocol.packagedata import EnodoRequest

from .modules import module_load

AVAILABLE_LOG_LEVELS = {
    'error': logging.ERROR, 'warning': logging.WARNING,
    'info': logging.INFO, 'debug': logging.DEBUG}


def prepare_logger(log_level_label):
    log_level = AVAILABLE_LOG_LEVELS.get(log_level_label)
    logger = logging.getLogger()
    logger.setLevel(log_level)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt='[%(levelname)1.1s %(asctime)s hub] ' +
            '%(message)s',
        datefmt='%y%m%d %H:%M:%S',
        style='%')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


class HubClient:

    def __init__(self, hub_id, writer, worker_config: dict):
        self.hub_id = hub_id
        self.writer = writer
        self.worker_config = WorkerConfigModel(**worker_config)


class SeriesState(dict):

    def __init__(self, series_name, state, last_used=None):
        super().__init__({
            'series_name': series_name,
            'state': state,
            'last_used': last_used or time()
        })

    @property
    def series_name(self):
        return self['series_name']

    @property
    def state(self):
        return self['state']

    @property
    def last_used(self):
        return self['last_used']

    def update(self, state):
        self['state'] = state
        self['last_used'] = time()

    @classmethod
    def upsert(cls, obj, series_name, state):
        if series_name in obj:
            return obj[series_name].update(state)
        obj[series_name] = cls(series_name, state)
        return obj[series_name]


class WorkerServer:
    def __init__(
            self, log_level, worker_name, worker_version, state_path):
        prepare_logger(log_level)
        self._config = EnodoConfigParser()
        self._hostname = self._config.get('WORKER_HOSTNAME')
        self._port = int(self._config.get('WORKER_PORT'))
        self._state_path = state_path
        self._server = None
        self._server_coro = None
        self._server_running = False
        self._job_type = None
        self._background_task = None

        self._clients = {}
        self._settings = WorkerConfigModel({}, [])

        self._open_jobs = Queue()
        self._job_results = MQueue()
        self._logging_queue = MQueue()
        self._busy = False
        self._states = {}
        self._state_lock = asyncio.Lock()
        self._modules = {}
        self._module_classes = {}
        self._cleanup_task = None
        self._worker_process = None
        self._keep_state = False

        logging.info(f"Starting {worker_name} V{worker_version}, "
                     f"with lib V{__version__}")

    async def create(self, loop=None):
        loop = loop or asyncio.get_running_loop()
        self._server_running = True
        self._server = await asyncio.start_server(
            self._handle_client_connection, self._hostname, self._port,
            loop=loop)

    async def stop(self):
        self._server_running = False
        await self._server.close()

    async def _cleanup(self):
        while self._server_running:
            await asyncio.sleep(get_dt_to_midnight())
            async with self._state_lock:
                now = time()
                for series_name in list(self._states.keys()):
                    state = self._states[series_name]
                    if state.last_used < now - 43200:
                        # If not used in last 12 hours, cleanup
                        del self._states[series_name]

    async def _handle_client_connection(self, reader, writer):
        connected = True
        saved_client_id = None
        while connected and self._server_running:
            packet_type, _, _, data = await read_packet(reader)
            if data is False:
                connected = False
                continue
            if len(data):
                data = qpack.unpackb(data, decode='utf-8')

            addr = writer.get_extra_info('peername')
            logging.debug("Received %r from %r" % (packet_type, addr))
            if packet_type == HANDSHAKE:
                saved_client_id, connected = await self._handle_handshake(
                    addr, writer, data)
            elif packet_type == HEARTBEAT:
                await self._handle_heartbeat(addr, writer, data)
            elif packet_type == CLIENT_SHUTDOWN:
                await self._handle_client_shutdown(saved_client_id)
                connected = False
            elif packet_type in [WORKER_REQUEST, WORKER_QUERY]:
                if packet_type == WORKER_QUERY:
                    data['hub_id'] = saved_client_id
                try:
                    request = EnodoRequest(**data)
                except Exception as e:
                    logging.error("Could not deserialize request")
                    continue
                await self._handle_request(saved_client_id, request, writer)
            elif packet_type == WORKER_REQUEST_RESULT:
                await self._handle_request_result(data)
            else:
                logging.error(
                    f'Package type {packet_type} not implemented')

        logging.info(f'Closing socket with client {saved_client_id}')
        writer.close()

    async def _handle_request_result(self, data):
        pass

    async def _work_queues(self):
        while self._server_running:
            await asyncio.sleep(0.1)
            if not self._job_results.empty():
                await self._work_result_queue()
            if not self._open_jobs.empty():
                if isinstance(self._worker_process, Process) and \
                        self._worker_process.is_alive():
                    continue
                next_request = self._open_jobs.get()
                await self._handle_open_request(next_request)
            if not self._logging_queue.empty():
                log_entry = self._logging_queue.get()
                log_method = getattr(logging, log_entry['level'])
                try:
                    log_method(log_entry['msg'])
                except:
                    logging.error("Could not log from queue")

    async def _handle_open_request(self, next_request):
        request = next_request.get('request')
        async with self._state_lock:
            series_state = self._states.get(request.get('series_name'))
        if series_state is not None:
            series_state = series_state.state
        self._do_job(request, series_state)

    async def _upsert_state(self, series_name, result):
        async with self._state_lock:
            SeriesState.upsert(self._states, series_name, result)

    async def _work_result_queue(self):
        result = self._job_results.get()
        request = result['request']
        if self._keep_state:
            await self._upsert_state(
                request.get('series_nane'), result['result'])
        if 'error' in result:
            event = EnodoEvent(
                "Error occured in worker", result['error'],
                ENODO_EVENT_WORKER_ERROR, request.get('series_name'))
            event = qpack.packb(event)
            response = create_header(len(event), EVENT)
            self._clients[request.get('hub_id')].writer.write(response + event)
            await self._clients[request.get('hub_id')].writer.drain()
        if request.get('request_type') == "fetch":
            data = {
                "data": result['result'],
                "request": request
            }
            await self.send_response_to_hub(
                data, request['pool_id'], request['worker_id'])

    async def send_response_to_hub(self, response, pool_id, worker_id):
        if len(self._clients) == 0:
            return
        if len(self._clients) == 1:
            hub_client = self._clients.values()[0]
        else:
            hub_client = list(self._clients.values())[
                randrange(0, len(self._clients) - 1)]
        data = qpack.packb(response)
        header = create_header(len(data), WORKER_REQUEST_RESULT) + \
            pool_id.to_bytes(4, byteorder='big') + \
            worker_id.to_bytes(1, byteorder='big')
        hub_client.writer.write(header + response)
        await hub_client.writer.drain()

    async def send_request_to_hub(self, request):
        if len(self._clients) == 0:
            return
        if len(self._clients) == 1:
            hub_client = self._clients.values()[0]
        else:
            hub_client = list(self._clients.values())[
                randrange(0, len(self._clients) - 1)]
        data = qpack.packb(request)
        header = create_header(len(data), WORKER_REQUEST)
        hub_client.writer.write(header + request)
        await hub_client.writer.drain()

    def _do_job(self, data, state):
        try:
            self._worker_process = Process(
                target=start_analysing,
                args=(self._job_results, self._logging_queue, data, state,
                      {"host": self._config.get('SIRIDB_DATA_HOST'),
                       "port": int(self._config.get('SIRIDB_DATA_PORT')),
                       "user": self._config.get('SIRIDB_DATA_USER'),
                       "password": self._config.get('SIRIDB_DATA_PASSWORD'),
                       "database": self._config.get('SIRIDB_DATA_DATABASE')},
                      {"host": self._config.get('SIRIDB_OUTPUT_HOST'),
                       "port": int(self._config.get('SIRIDB_OUTPUT_PORT')),
                       "user": self._config.get('SIRIDB_OUTPUT_USER'),
                       "password": self._config.get('SIRIDB_OUTPUT_PASSWORD'),
                       "database": self._config.get('SIRIDB_OUTPUT_DATABASE')},
                      self._module_classes))
            # Start the thread
            self._worker_process.start()
        except Exception as e:
            logging.error('Error while creating worker thread')
            logging.debug(
                f'Corresponding error: {str(e)}, '
                f'exception class: {e.__class__.__name__}')

    async def _handle_request(
            self, hub_id, request: EnodoRequest, writer):
        if request.get('request_type') in ["run", "fetch"]:
            self._open_jobs.put({
                "request": request,
                "origin": hub_id
            })
        elif request.get('request_type') == "query":
            async with self._state_lock:
                series_state = self._states.get(request.get('series_name'))
                if series_state is not None:
                    series_state = dict(series_state.state)
                data = {
                    "data": series_state,
                    "request_id": request.get('request_id')
                }
                data = qpack.packb(data)
                response = create_header(len(data), WORKER_QUERY_RESULT)
                writer.write(response + data)
                await writer.drain()

    async def _handle_client_shutdown(self, client_id):
        logging.info(f"Connection with hub {client_id} was closed")
        del self._clients[client_id]

    async def _handle_heartbeat(self, addr, writer, data):
        if addr in self._clients:
            response = create_header(0, HEARTBEAT)
        else:
            response = create_header(0, UNKNOWN_CLIENT)
        writer.write(response)
        await writer.drain()

    async def _handle_handshake(self, addr, writer, data):
        client_data = data
        worker_config = client_data.get('worker_config')

        if worker_config['job_type'] != self._job_type:
            logging.error("Hub connected with config "
                          "which has invalid job types set")
            response = create_header(0, HANDSHAKE_FAIL)
            writer.write(response)
            await writer.drain()
            return addr, False

        self._clients[addr] = HubClient(addr, writer, worker_config)
        self._settings = self._clients[addr].worker_config

        response = create_header(0, HANDSHAKE_OK)
        writer.write(response)
        await writer.drain()
        return addr, True

    def load_module(self, base_dir):
        # Get installed module
        modules = module_load(base_dir)
        logging.info("Loading installed analysis module:")
        for module_name, module_class in modules.items():
            self._modules[module_name] = module_class.get_module_info()
            self._module_classes[module_name] = module_class
            self._job_type = module_class.get_module_info().job_type
            logging.info(f" - {module_name}")
            return  # TODO: Only once so just get first item instead of loop

    def _load_state(self):
        try:
            with open(self._state_path, 'r') as f:
                data = f.read()
                data = json.loads(data)
        except Exception as e:
            logging.error("Could not load state from disk on startup")
            logging.debug(f"Corresponding error: {str(e)}")
        else:
            for series_name, state in data.items():
                self._states[series_name] = SeriesState(series_name, state)

    async def start_worker(self):
        if len(self._modules.keys()) < 1:
            logging.error("No module loaded")
            return

        self._loop = asyncio.get_running_loop()

        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            self._loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(
                    self.shutdown(s)))

        self._states = {}
        if self._keep_state:
            self._load_state()

        await self.create()
        self._background_task = self._loop.create_task(self._work_queues())
        self._cleanup_task = self._loop.create_task(self._cleanup())

        await asyncio.gather(self._background_task)
        await asyncio.gather(self._cleanup_task)

    async def close(self):
        logging.info('Closing the socket')
        self._running = False
        if self._background_task:
            self._background_task.cancel()

    async def shutdown(self, s):
        self._running = False
        # TODO: Send shutdown msg
        # await self._client.send_message(None, CLIENT_SHUTDOWN)
        if self._keep_state:
            state = {}
            for series_state in self._states.values():
                state[series_state.series_name] = series_state.state

            try:
                with open(self._state_path, 'w') as f:
                    f.write(json.dumps(state))
            except Exception as e:
                logging.error("Could not save state to disk on shutdown")
                logging.debug(f"Corresponding error: {str(e)}")

        await self.close()

        """Cleanup tasks tied to the service's shutdown."""
        logging.info(f"Received exit signal {s.name}...")
        logging.info("Sending Hub that we are going down")
        tasks = [t for t in asyncio.all_tasks()
                 if t is not asyncio.current_task()]

        [task.cancel() for task in tasks]

        logging.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks)
        await asyncio.sleep(1)
        self._loop.stop()
