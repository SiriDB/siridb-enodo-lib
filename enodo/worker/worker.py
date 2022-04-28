import asyncio
import datetime
import os
import signal
import logging

from multiprocessing import Process, Queue

from enodo.client import Client
from enodo.protocol.package import *
from enodo.jobs import JOB_TYPE_FORECAST_SERIES, \
    JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, JOB_TYPE_BASE_SERIES_ANALYSIS, \
    JOB_TYPE_STATIC_RULES
from enodo.protocol.packagedata import EnodoJobDataModel
from enodo.model.config.series import SeriesJobConfigModel

from .analyser import start_analysing
from .modules import module_load
from .lib.config import EnodoConfigParser
from .lib.logging import prepare_logger
from enodo.version import __version__ as VERSION


class Worker:

    def __init__(self, config_path, log_level):
        self._loop = None
        self._log_level = log_level
        self._config = EnodoConfigParser()
        if config_path is not None and os.path.exists(config_path):
            self._config.read(config_path)
        self._client = None

        self._client_run_task = None
        self._updater_task = None
        self._result_queue = Queue()
        self._busy = False
        self._started_last_job = None
        self._max_job_duration = self._config['worker'][
            'max_job_duration']
        self._worker_process = None
        self._current_job = None
        self._running = True
        self._current_job_started_at = None
        self._modules = {}
        self._module_classes = {}

        prepare_logger(self._log_level)

    async def _update_busy(self, busy, job_id=None):
        self._busy = busy
        self._current_job = job_id
        self._current_job_started_at = \
            datetime.datetime.now() if busy else None
        await self._client.send_message(busy, WORKER_UPDATE_BUSY)

    async def _send_refused(self):
        await self._client.send_message(None, WORKER_REFUSED)

    async def _send_shutdown(self):
        await self._client.send_message(None, CLIENT_SHUTDOWN)

    async def _check_for_update(self):
        while self._running:
            if not self._result_queue.empty():
                try:
                    result = self._result_queue.get()
                except Exception as e:
                    logging.error(
                        'Error while fetching item from result queue')
                    logging.debug(
                        f'Corresponding error: {str(e)}, '
                        f'exception class: {e.__class__.__name__}')
                else:
                    result['job_id'] = self._current_job
                    await self._send_update(result)
                    await self._update_busy(False)
            if self._busy and (
                datetime.datetime.now() - self._current_job_started_at)\
                    .total_seconds() >= int(self._max_job_duration):
                await self._cancel_job()
            await asyncio.sleep(2)

    async def _send_update(self, pkl):
        try:
            await self._client.send_message(pkl, WORKER_JOB_RESULT)
        except Exception as e:
            logging.error('Error while sending update to Hub')
            logging.debug(f'Corresponding error: {str(e)}, '
                          f'exception class: {e.__class__.__name__}')

    async def _receive_job(self, data):
        if self._busy:
            await self._send_refused()
        else:
            try:
                data = EnodoJobDataModel.unserialize(data)
                logging.info(
                    f'Received request for '
                    f'{data.get("job_config").get("job_type")} for series: '
                    f'"{data.get("series_name")}"')
            except Exception as e:
                logging.error(
                    'Error while unserializing incoming job data')
                logging.debug(
                    f'Corresponding error: {str(e)}',
                    f'exception class: {e.__class__.__name__}')
            await self._update_busy(True, data.get('job_id'))
            job_config = SeriesJobConfigModel(**data.get('job_config'))
            job_type = job_config.job_type

            if job_type in [JOB_TYPE_FORECAST_SERIES,
                            JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES,
                            JOB_TYPE_BASE_SERIES_ANALYSIS,
                            JOB_TYPE_STATIC_RULES]:
                module_name = job_config.module
                if not await self._check_support_job_and_module(
                        job_type, module_name):
                    await self._send_update(
                        {'error': 'Unsupported module for job_type',
                         'job_id': data.get('job_id'),
                         'name': data.get("series_name")})
                    await self._update_busy(False)
                    return
            else:
                await self._send_update(
                    {'error': 'Unsupported job_type',
                     'job_id': data.get('job_id'),
                     'name': data.get("series_name")})
                await self._update_busy(False)
                return

            try:

                self._worker_process = Process(
                    target=start_analysing,
                    args=(
                        self._result_queue, data,
                        self._config['siridb_data'],
                        self._config['siridb_output'],
                        self._module_classes
                    ))
                # Start the thread
                # self._worker_thread.daemon = True
                self._worker_process.start()
            except Exception as e:
                logging.error('Error while creating worker thread')
                logging.debug(
                    f'Corresponding error: {str(e)}, '
                    f'exception class: {e.__class__.__name__}')

    async def _check_support_job_and_module(self, job_type, module_name=None):
        modules = self._modules.values()
        if module_name is not None:
            modules = [self._modules.get(module_name)]
        for module in modules:
            if module.support_job_type(job_type):
                return True
        return False

    async def _cancel_job(self):
        try:
            self._worker_process.kill()
        except Exception as e:
            logging.error('Error while trying to cancel job')
            logging.debug(f'Corresponding error: {str(e)}, '
                          f'exception class: {e.__class__.__name__}')
        finally:
            await self._send_job_cancelled()

    async def _receive_to_cancel_job(self, data):
        job_id = data.get('job_id')
        if job_id is self._current_job:
            await self._cancel_job()

    async def _send_job_cancelled(self):
        await self._client.send_message({
            "job_id": self._current_job}, WORKER_JOB_CANCELLED)
        await self._update_busy(False)

    async def _add_handshake_data(self):
        serialized_modules = [self._modules[module]
                              for module in self._modules]

        return {'busy': self._busy,
                'modules': serialized_modules}

    def load_module(self, base_dir):
        # Get installed module
        modules = module_load(base_dir)
        logging.info("Loading installed analysis module:")
        for module_name, module_class in modules.items():
            self._modules[module_name] = module_class.get_module_info()
            self._module_classes[module_name] = module_class
            logging.info(f" - {module_name}")

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

        self._client = Client(
            self._loop, self._config['worker']['hub_hostname'],
            int(self._config['worker']['hub_port']),
            'worker', self._config['worker']
            ['internal_security_token'],
            heartbeat_interval=int(
                self._config['worker']['heartbeat_interval']),
            identity_file_path=".enodo_id", client_version=VERSION)

        await self._client.setup(cbs={
            WORKER_JOB: self._receive_job,
            WORKER_JOB_CANCEL: self._receive_to_cancel_job
        },
            handshake_cb=self._add_handshake_data)
        self._client_run_task = self._loop.create_task(
            self._client.run())
        self._updater_task = self._loop.create_task(
            self._check_for_update())

        await asyncio.gather(self._client_run_task)

    async def shutdown(self, s):
        self._running = False
        await self._send_shutdown()
        await self._client.close()

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
