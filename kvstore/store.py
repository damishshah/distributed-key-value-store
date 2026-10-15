"""Durable in-memory key-value store backed by a snapshot plus WAL.

Durability protocol:

1. Assign the next sequence number.
2. Append the mutation to the write-ahead log and fsync.
3. Apply the mutation to the in-memory map.

Recovery loads the latest snapshot, then replays WAL records with a sequence
number greater than the snapshot's. A snapshot rotates the WAL so the log
does not grow without bound.
"""

from __future__ import annotations

import os
import threading
from typing import Any

from kvstore.snapshot import SnapshotStore
from kvstore.wal import DEL, SET, WriteAheadLog


class DurableKVStore:
    def __init__(
        self,
        data_dir: str,
        *,
        fsync: bool = True,
        snapshot_every: int | None = None,
    ) -> None:
        os.makedirs(data_dir, exist_ok=True)
        self._lock = threading.RLock()
        self._snapshot = SnapshotStore(os.path.join(data_dir, "snapshot.json"))
        self._wal = WriteAheadLog(os.path.join(data_dir, "wal.log"), fsync=fsync)
        self._snapshot_every = snapshot_every
        self._ops_since_snapshot = 0
        self._data: dict[str, Any] = {}
        self._seq = 0
        self._recover()

    def _recover(self) -> None:
        data, seq = self._snapshot.load()
        self._data = dict(data)
        self._seq = seq
        for record in self._wal.replay():
            record_seq = record.get("seq")
            if not isinstance(record_seq, int):
                continue
            if record_seq <= self._seq:
                continue
            self._apply(record)
            self._seq = record_seq

    def _apply(self, record: dict[str, Any]) -> None:
        op = record.get("op")
        key = record.get("key")
        if not isinstance(key, str):
            return
        if op == SET:
            self._data[key] = record.get("value")
        elif op == DEL:
            self._data.pop(key, None)

    def _append_and_apply(self, op: str, key: str, value: Any = None) -> None:
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        with self._lock:
            self._seq += 1
            record: dict[str, Any] = {"seq": self._seq, "op": op, "key": key}
            if op == SET:
                record["value"] = value
            self._wal.append(record)
            self._apply(record)
            self._ops_since_snapshot += 1
            if (
                self._snapshot_every is not None
                and self._ops_since_snapshot >= self._snapshot_every
            ):
                self._snapshot_locked()

    def get(self, key: str) -> Any:
        with self._lock:
            if key not in self._data:
                raise KeyError(key)
            return self._data[key]

    def set(self, key: str, value: Any) -> None:
        self._append_and_apply(SET, key, value)

    def delete(self, key: str) -> None:
        self._append_and_apply(DEL, key)

    def _snapshot_locked(self) -> None:
        self._snapshot.save(dict(self._data), self._seq)
        self._wal.rotate()
        self._ops_since_snapshot = 0

    def items(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._data)

    def close(self) -> None:
        with self._lock:
            self._wal.close()

    def __enter__(self) -> DurableKVStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
