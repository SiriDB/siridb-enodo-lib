import asyncio
import socket

HANDSHAKE = 1
HANDSHAKE_OK = 2
HANDSHAKE_FAIL = 3
UNKNOWN_CLIENT = 4
HEARTBEAT = 5
SHUTDOWN = 6
CLIENT_SHUTDOWN = 7

ADD_SERIES = 8
REMOVE_SERIES = 9
LISTENER_ADD_SERIES = 10
LISTENER_REMOVE_SERIES = 11
LISTENER_NEW_SERIES_POINTS = 12
UPDATE_SERIES = 13

RESPONSE_OK = 14

WORKER_JOB = 15
WORKER_JOB_RESULT = 16
WORKER_JOB_CANCEL = 21
WORKER_JOB_CANCELLED = 22
WORKER_UPDATE_BUSY = 23
WORKER_REFUSED = 24


'''
Header:
size,     int,    32bit
type,     int     8bit
packetid, int     8bit

total header length = 48 bits == 6 bytes
'''

PACKET_HEADER_LEN = 6


async def read_packet(sock, header_data=None):
    if header_data is None:
        header_data = await read_full_data(sock, PACKET_HEADER_LEN)
    if header_data is False:
        return None, None, False
    body_size, packet_type, packet_id = read_header(header_data)
    return packet_type, packet_id, await read_full_data(sock, body_size)


async def read_full_data(sock, data_size):
    r_data = bytearray()
    while True:
        chunk = await sock.read(data_size - len(r_data))
        r_data += chunk
        if len(r_data) == data_size:
            break
        if len(r_data) == 0:
            return False
        await asyncio.sleep(0.01)
    return r_data


def create_header(size, type, id=1):
    return \
        size.to_bytes(4, byteorder='big') + \
        type.to_bytes(1, byteorder='big') + \
        id.to_bytes(1, byteorder='big')


def read_header(binary_data):
    return \
        int.from_bytes(binary_data[:4], 'big'), \
        int.from_bytes(binary_data[4:5], 'big'), \
        int.from_bytes(binary_data[5:6], 'big')
