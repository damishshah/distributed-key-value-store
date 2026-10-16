"""Append-only write-ahead log with checksummed records.

Each record is:

    [uint32 payload_len][uint32 crc32][payload bytes]

The payload is UTF-8 JSON: ``{"seq": int, "op": "SET"|"DEL", "key": str, "value": ...}``.
A torn or corrupt tail is treated as the end of the log so recovery can stop
cleanly after a crash mid-write.
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from typing import Any, Iterator

HEADER_FORMAT = "!II"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

SET = "SET"
DEL = "DEL"


class WALError(Exception):
    """Raised when a WAL record is structurally invalid."""


class WriteAheadLog:
    def __init__(self, path: str, *, fsync: bool = True) -> None:
        self.path = path
        self.fsync = fsync
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._file = open(path, "ab+")

    def append(self, record: dict[str, Any]) -> None:
        payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        crc = zlib.crc32(payload)
        self._file.write(struct.pack(HEADER_FORMAT, len(payload), crc))
        self._file.write(payload)
        self._file.flush()
        if self.fsync:
            os.fsync(self._file.fileno())

    def replay(self) -> Iterator[dict[str, Any]]:
        """Yield valid records in order. Stop at the first incomplete/corrupt tail."""
        with open(self.path, "rb") as handle:
            while True:
                header = handle.read(HEADER_SIZE)
                if not header:
                    return
                if len(header) < HEADER_SIZE:
                    return
                length, expected_crc = struct.unpack(HEADER_FORMAT, header)
                payload = handle.read(length)
                if len(payload) < length:
                    return
                actual_crc = zlib.crc32(payload)
                if actual_crc != expected_crc:
                    return
                try:
                    record = json.loads(payload.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise WALError("WAL payload is not valid JSON") from exc
                yield record

    def rotate(self) -> None:
        """Replace the log with a new empty file after a successful snapshot."""
        self.close()
        os.replace(self.path, self.path + ".old")
        self._file = open(self.path, "ab+")
        try:
            os.remove(self.path + ".old")
        except FileNotFoundError:
            pass

    def close(self) -> None:
        if self._file.closed:
            return
        self._file.flush()
        if self.fsync:
            try:
                os.fsync(self._file.fileno())
            except OSError:
                pass
        self._file.close()

    def __enter__(self) -> WriteAheadLog:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
