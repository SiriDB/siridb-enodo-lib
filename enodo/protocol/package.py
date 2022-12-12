import asyncio


'''
Header:
size,     int,    32bit
type,     int     8bit

total header length = 40 bits == 5 bytes
'''

PACKET_HEADER_LEN = 5


async def read_packet(sock, header_data=None):
    pool_id = worker_id = None
    if header_data is None:
        header_data = await read_full_data(sock, PACKET_HEADER_LEN)
    if header_data is False:
        return None, None, None, False
    body_size, packet_type = read_header(header_data)
    return packet_type, pool_id, worker_id, await read_full_data(sock, body_size)


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


def create_header(size, type):
    return size.to_bytes(4, byteorder='big') + \
        type.to_bytes(1, byteorder='big')


def read_header(binary_data):
    return \
        int.from_bytes(binary_data[:4], 'big'), \
        int.from_bytes(binary_data[4:5], 'big')


def read_extended_header(binary_data):
    return \
        int.from_bytes(binary_data[:1], 'big'), \
        int.from_bytes(binary_data[1:2], 'big')
