"""Atomic snapshot of the in-memory key-value map.

Snapshots are JSON files written to a temp path, flushed, then renamed into
place so a crash cannot leave a half-written snapshot as the current one.
"""

from __future__ import annotations

import json
import os
from typing import Any

SNAPSHOT_VERSION = 1


class SnapshotError(Exception):
    """Raised when a snapshot file cannot be loaded."""


class SnapshotStore:
    def __init__(self, path: str) -> None:
        self.path = path
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

    def save(self, data: dict[str, Any], seq: int) -> None:
        payload = {
            "version": SNAPSHOT_VERSION,
            "seq": seq,
            "data": data,
        }
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, self.path)

    def load(self) -> tuple[dict[str, Any], int]:
        """Return ``(data, seq)``. Missing file yields an empty store at seq 0."""
        if not os.path.exists(self.path):
            return {}, 0
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise SnapshotError(f"failed to load snapshot {self.path}") from exc
        if payload.get("version") != SNAPSHOT_VERSION:
            raise SnapshotError(
                f"unsupported snapshot version {payload.get('version')}"
            )
        data = payload.get("data")
        seq = payload.get("seq")
        if not isinstance(data, dict) or not isinstance(seq, int):
            raise SnapshotError("snapshot is missing data or seq")
        return data, seq
