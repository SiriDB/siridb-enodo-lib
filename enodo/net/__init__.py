from .package import Package
from .protocol import Protocol as BaseProtocol

PROTO_REQ_HANDSHAKE = 0x01
PROTO_REQ_HEARTBEAT = 0x02
PROTO_REQ_SHUTDOWN = 0x03
PROTO_REQ_LISTENER_ADD_SERIES = 0x04
PROTO_REQ_LISTENER_REMOVE_SERIES = 0x05
PROTO_REQ_LISTENER_NEW_SERIES_POINTS = 0x06
PROTO_REQ_UPDATE_SERIES = 0x07
PROTO_REQ_WORKER_REQUEST = 0x08
PROTO_REQ_WORKER_REQUEST_REDIRECT = 0x09
PROTO_REQ_EVENT = 0x0a


PROTO_RES_HANDSHAKE_OK = 0x40
PROTO_RES_HANDSHAKE_FAIL = 0x41
PROTO_RES_UNKNOWN_CLIENT = 0x42
PROTO_RES_HEARTBEAT = 0x43
PROTO_RES_WORKER_REQUEST = 0x44
PROTO_RES_WORKER_REQUEST_REDIRECT = 0x45