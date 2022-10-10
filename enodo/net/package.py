from __future__ import annotations
import logging
import qpack
import struct
from typing import Any, Optional


class Package(object):

    __slots__ = ('length', 'tp', 'body', 'data', 'total')

    st_package = struct.Struct('<IBB')

    def __init__(self, barray: Optional[bytearray] = None):
        if barray is None:
            return

        self.length, self.tp, checkbit = \
            self.__class__.st_package.unpack_from(barray, offset=0)
        if self.tp != checkbit ^ 255:
            raise ValueError('invalid checkbit')
        self.total = self.__class__.st_package.size + self.length
        self.body = None
        self.data = None

    @classmethod
    def make(
        cls,
        tp: int,
        data: bytes = b'',
        is_binary: bool = False,
    ) -> Package:
        pkg = cls()
        pkg.tp = tp

        if is_binary is False:
            data = qpack.packb(data)

        pkg.data = data
        pkg.length = len(data)
        return pkg

    def to_bytes(self) -> bytes:
        header = self.st_package.pack(
            self.length,
            self.tp,
            self.tp ^ 0xff)

        return header + self.data

    def extract_data_from(self, barray: bytearray):
        self.body = None
        try:
            if self.length:
                self.body = barray[self.__class__.st_package.size:self.total]
        finally:
            del barray[:self.total]

    def partial_read(self, until: int) -> Any:
        if self.body is None:
            return
        if len(self.body) < until:
            return
        try:
            partial_data = qpack.unpackb(
                self.body[: until],
                decode="utf-8")
        except Exception:
            logging.error('failed to unpack package id: {0.pid}'.format(self))
            raise
        else:
            del self.body[:until]
        return partial_data

    def read_data(self) -> Any:
        if self.data:
            return self.data
        if self.body is None:
            return
        try:
            self.data = qpack.unpackb(self.body, decode='utf-8')
        except Exception:
            logging.error('failed to unpack package id: {0.pid}'.format(self))
            raise
        self.body = None
        return self.data

    def __repr__(self) -> str:
        return '<id: {0.pid} size: {0.length} tp: {0.tp}>'.format(self)
