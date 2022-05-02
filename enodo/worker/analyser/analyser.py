import asyncio
import logging
import traceback

from enodo.jobs import JOB_TYPE_FORECAST_SERIES, \
    JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES, \
    JOB_TYPE_BASE_SERIES_ANALYSIS, JOB_TYPE_STATIC_RULES
from enodo.model.config.series import SeriesJobConfigModel
from enodo.protocol.packagedata import EnodoJobDataModel

from .lib.siridb.siridb import SiriDB


class Analyser:
    _analyser_queue = None
    _busy = None
    _siridb_data_client = None
    _siridb_output_client = None
    _shutdown = None
    _current_future = None

    def __init__(
            self, queue, siridb_data, siridb_output, modules):
        self._siridb_data_client = self._siridb_output_client = SiriDB(
            siridb_data['user'],
            siridb_data['password'],
            siridb_data['database'],
            siridb_data['host'],
            siridb_data['port'])
        if siridb_output != siridb_data:
            self._siridb_output_client = SiriDB(
                siridb_output['user'],
                siridb_output['password'],
                siridb_output['database'],
                siridb_output['host'],
                siridb_output['port'])
        self._analyser_queue = queue
        self._modules = modules

    async def query_siridb(self, query, output=False):
        if output:
            return await self._siridb_output_client.run_query(query)
        return await self._siridb_data_client.run_query(query)

    async def execute_job(self, job_data):
        series_name = job_data.get("series_name")
        series_state = job_data.get("series_state")
        job_config = SeriesJobConfigModel(**job_data.get('job_config'))
        max_n_points = job_config.get('max_n_points')
        time_unit = job_data.get("time_unit")
        job_type = job_config.job_type
        timeval = None

        time_unit = (await self._siridb_data_client.run_query(
            "show time_precision"))["data"][0]["value"]

        series_data = await self._siridb_data_client.query_series_data(
            series_name, since=timeval)

        # if max_n_points is not None and \
        #         series_state.get("interval") is not None and \
        #         time_unit is not None:
        #     timeval = max_n_points * int(series_state.get("interval"))
        #     if time_unit == "ms":
        #         timeval = int(timeval / 1000)
        #     timeval = int(timeval / 60)  # In minutes

        if series_name not in series_data:
            return self._analyser_queue.put(
                {'name': series_name,
                 'error': 'Cannot find series data'})
        # TODO: use tail function
        dataset = series_data[series_name][-1000:]
        parameters = job_config.module_params

        module_class = self._modules.get(job_config.module)

        if module_class is not None:
            module = module_class(dataset, parameters,
                                  series_name, self.query_siridb)

            if job_type == JOB_TYPE_BASE_SERIES_ANALYSIS:
                await self._analyse_series(series_name, module)
            elif job_type == JOB_TYPE_STATIC_RULES:
                await self._check_static_rules(series_name, module)
            elif job_type == JOB_TYPE_FORECAST_SERIES:
                await self._forcast_series(series_name, module)
            elif job_type == JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES:
                await self._detect_anomalies(series_name, module)
            else:
                self._analyser_queue.put(
                    {'name': series_name,
                     'error': 'Job type not implemented'})
        else:
            self._analyser_queue.put(
                {'name': series_name,
                 'error': 'Module not implemented'})

    def _handle_response_to_queue(self, response_data):
        if not EnodoJobDataModel.validate_by_job_type(
                response_data, response_data.get('job_type')):
            self._analyser_queue.put(
                {'name': response_data.get('name'),
                 'error': 'Job response not valid'})
            return
        self._analyser_queue.put(response_data)

    async def _analyse_series(self, series_name, analysis_module):
        response = await analysis_module.do_base_analysis()
        self._handle_response_to_queue(
            {'name': series_name,
             'job_type': JOB_TYPE_BASE_SERIES_ANALYSIS, **response})

    async def _check_static_rules(self, series_name, analysis_module):
        response = await analysis_module.do_static_rules_check()
        self._handle_response_to_queue({
            'name': series_name,
            'job_type': JOB_TYPE_STATIC_RULES,
            **response})

    async def _forcast_series(self, series_name, analysis_module):
        """
        Collects data for starting an analysis of a specific time serie
        :param series_name:
        :return:
        """
        error = None
        response = []
        try:
            response = await analysis_module.do_forecast()
        except Exception as e:
            tb = traceback.format_exc()
            error = f"{str(e)}, tb: {tb}"
            logging.error(
                'Error while making and executing forcast module')
            logging.debug(f'Corresponding error: {error}, '
                          f'exception class: {e.__class__.__name__}')
        finally:
            if error is not None:
                self._analyser_queue.put(
                    {'name': series_name,
                     'job_type': JOB_TYPE_FORECAST_SERIES,
                     'error': error})
            else:
                self._handle_response_to_queue(
                    {'name': series_name,
                     'job_type': JOB_TYPE_FORECAST_SERIES,
                     **response})

    async def _detect_anomalies(self, series_name, analysis_module):
        error = None
        response = {}
        try:
            response = await analysis_module.do_anomaly_detect()
        except Exception as e:
            tb = traceback.format_exc()
            error = f"{str(e)}, tb: {tb}"
            logging.error(
                'Error while making and executing anomaly detection module')
            logging.debug(f'Corresponding error: {error}, '
                          f'exception class: {e.__class__.__name__}')
        finally:
            if error is not None:
                self._analyser_queue.put(
                    {'name': series_name,
                     'job_type': JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES,
                     'error': error})
            else:
                self._handle_response_to_queue(
                    {'name': series_name,
                     'job_type': JOB_TYPE_DETECT_ANOMALIES_FOR_SERIES,
                     **response})


async def _save_start_with_timeout(queue, job_data,
                                   siridb_data, siridb_output, modules):
    try:
        analyser = Analyser(queue, siridb_data, siridb_output, modules)
        await analyser.execute_job(job_data)
    except Exception as e:
        tb = traceback.format_exc()
        error = f"{str(e)}, tb: {tb}"
        logging.error('Error while executing Analyzer')
        logging.error(f'Corresponding error: {error}, '
                      f'exception class: {e.__class__.__name__}')


def start_analysing(
        queue, job_data, siridb_data, siridb_output, modules):
    """Switch to new event loop and run forever"""

    try:
        asyncio.run(_save_start_with_timeout(
            queue, job_data, siridb_data, siridb_output, modules))
    except Exception as e:
        logging.error('Error while starting Analyzer')
        logging.debug(f'Corresponding error: {e}, '
                      f'exception class: {e.__class__.__name__}')
        exit()
