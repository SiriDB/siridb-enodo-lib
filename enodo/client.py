import asyncio
import datetime
import os
import uuid

import qpack

from enodo.logging import client_logger as logger
from .protocol.package import *
from .version import __version__ as VERSION


class EnodoStreamReaderProtocol(asyncio.StreamReaderProtocol):

    def __init__(
            self, stream_reader, client_connected_cb=None, loop=None,
            client_lost_connection_cb=None):
        super(EnodoStreamReaderProtocol, self).__init__(
            stream_reader, client_connected_cb=None, loop=None)
        self._client_lost_connection_cb = client_lost_connection_cb

    def connection_lost(self, exc):
        super(EnodoStreamReaderProtocol, self).connection_lost(exc)
        if self._client_lost_connection_cb is not None:
            self._client_lost_connection_cb()


async def open_connection(host=None, port=None,
                          limit=asyncio.streams._DEFAULT_LIMIT,
                          client_lost_connection_cb=None):
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=limit, loop=loop)
    protocol = EnodoStreamReaderProtocol(
        reader, loop=loop,
        client_lost_connection_cb=client_lost_connection_cb)
    transport, _ = await loop.create_connection(
        lambda: protocol, host, port)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


class Client:

    def __init__(self, loop, hostname, port, client_type, token,
                 identity_file_path=None, heartbeat_interval=5,
                 client_version=VERSION):
        self.loop = loop
        self._hostname = hostname
        self._port = port
        self._heartbeat_interval = heartbeat_interval
        self._client_type = client_type
        self._client_version = client_version
        self._id = uuid.uuid4().hex
        if identity_file_path is not None:
            enodo_id = self.read_enodo_id(identity_file_path)
            if enodo_id is None or enodo_id == "":
                self.write_enodo_id(identity_file_path, self._id)
            else:
                self._id = enodo_id

        self._token = token
        self._last_heartbeat_send = datetime.datetime.now()
        self._last_heartbeat_received = datetime.datetime.now()
        self._cbs = None
        self._handshake_data_cb = None
        self._reader = None
        self._writer = None
        self._running = True
        self._connected = False
        self._handsshaked = False
        self._read_task = None

    async def setup(self, cbs=None, handshake_cb=None):
        self._cbs = cbs
        if cbs is None:
            self._cbs = {}
        if handshake_cb is not None:
            self._handshake_data_cb = handshake_cb

    def read_enodo_id(self, path):
        enodo_id = os.getenv(f"ENODO_ID")
        if not (enodo_id is None or enodo_id == ""):
            return enodo_id
        if not os.path.exists(path):
            return None
        with open(path, 'r') as f:
            enodo_id = f.read()
        return enodo_id

    def write_enodo_id(self, path, enodo_id):
        with open(path, 'w') as f:
            f.write(enodo_id)
        return True

    async def _connect(self):
        while not self._connected:
            logger.info("Trying to connect")
            try:
                self._reader, self._writer = await open_connection(
                    self._hostname,
                    self._port,
                    client_lost_connection_cb=self.connection_lost)
            except Exception as e:
                logger.warning(f"Cannot connect, {str(e)}")
                logger.info("Retrying in 5")
                await asyncio.sleep(4)
            else:
                logger.info("Connected")
                self._connected = True
                await self._handshake()

    def connection_lost(self):
        logger.warning('Connection lost')
        self._connected = False
        self._writer.close()
        self._writer = None
        self._read_task.cancel()

    async def run(self):
        while self._running:
            if not self._connected:
                if self._handsshaked:
                    self._handsshaked = False
                if self._writer and not self._writer.is_closing():
                    self._writer.close()
                    await self._writer.wait_closed()
                await self._connect()
            elif self._handsshaked:
                diff = datetime.datetime.now() - self._last_heartbeat_send
                if diff.total_seconds() > int(
                        self._heartbeat_interval):
                    await self._send_heartbeat()
                diff = datetime.datetime.now() - self._last_heartbeat_received
                if diff.total_seconds() > int(
                        2*self._heartbeat_interval):
                    logger.error(
                        "Haven't received heartback response from hub")
                    self._connected = False
            await asyncio.sleep(1)

    async def close(self):
        logger.info('Closing the socket')
        self._running = False
        if self._read_task:
            self._read_task.cancel()
        if self._writer:
            self._writer.close()

    async def _read_from_socket(self):
        while self._running:
            if not self._connected:
                await asyncio.sleep(1)
                continue

            packet_type, packet_id, data = await read_packet(self._reader)
            if data is False:
                self._connected = False
                continue

            if len(data):
                data = qpack.unpackb(data, decode='utf-8')

            if packet_type == 0:
                logger.warning("Connection lost, trying to reconnect")
                self._connected = False
                try:
                    await self.setup(self._cbs)
                except Exception as e:
                    logger.error('Error while trying to setup client')
                    logger.debug(f'Correspondig error: {str(e)}')
                    await asyncio.sleep(5)
            elif packet_type == HANDSHAKE_OK:
                logger.info(f'Hands shaked with hub')
                self._last_heartbeat_received = datetime.datetime.now()
                self._handsshaked = True
            elif packet_type == HANDSHAKE_FAIL:
                logger.error(f'Hub does not want to shake hands')
            elif packet_type == HEARTBEAT:
                logger.debug(f'Heartbeat back from hub')
                self._last_heartbeat_received = datetime.datetime.now()
            elif packet_type == RESPONSE_OK:
                logger.debug(f'Hub received update correctly')
            elif packet_type == UNKNOWN_CLIENT:
                logger.error(f'Hub does not recognize us')
                await self._handshake()
            else:
                if packet_type in self._cbs.keys():
                    await self._cbs.get(packet_type)(data)
                else:
                    logger.error(
                        f'Message type not implemented: {packet_type}')
            await asyncio.sleep(1)

    async def _send_message(self, length, message_type, data):
        header = create_header(length, message_type)

        logger.debug(f"Sending type: {message_type}")
        self._writer.write(header + data)
        try:
            await self._writer.drain()
        except Exception as e:
            self._connected = False

    async def send_message(self, body, message_type, use_qpack=True):
        if not self._connected:
            return False
        if use_qpack:
            body = qpack.packb(body)
        await self._send_message(len(body), message_type, body)

    async def _handshake(self):
        self._read_task = asyncio.Task(self._read_from_socket())
        data = {
            'client_id': str(self._id),
            'client_type': self._client_type,
            'token': self._token,
            'lib_version': self._client_version}
        if self._handshake_data_cb is not None:
            handshake_data = await self._handshake_data_cb()
            data = {**data, **handshake_data}
        data = qpack.packb(data)
        await self._send_message(len(data), HANDSHAKE, data)
        self._last_heartbeat_send = datetime.datetime.now()

    async def _send_heartbeat(self):
        logger.debug('Sending heartbeat to hub')
        id_encoded = qpack.packb(self._id)
        await self._send_message(len(id_encoded), HEARTBEAT, id_encoded)
        self._last_heartbeat_send = datetime.datetime.now()
