import json
import os
import struct
import tempfile
import unittest
import zlib

from kvstore.store import DurableKVStore
from kvstore.wal import HEADER_FORMAT, WriteAheadLog


class DurableKVStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.data_dir = self._tmpdir.name

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_set_get_delete(self) -> None:
        with DurableKVStore(self.data_dir, fsync=False) as store:
            store.set("a", 1)
            self.assertEqual(store.get("a"), 1)
            store.delete("a")
            with self.assertRaises(KeyError):
                store.get("a")
            store.delete("a")

    def test_wal_recovery_without_snapshot(self) -> None:
        with DurableKVStore(self.data_dir, fsync=False) as store:
            store.set("user", {"name": "ada"})
            store.set("count", 7)
            store.delete("count")
        with DurableKVStore(self.data_dir, fsync=False) as store:
            self.assertEqual(store.get("user"), {"name": "ada"})
            with self.assertRaises(KeyError):
                store.get("count")

    def test_snapshot_then_wal_replay(self) -> None:
        with DurableKVStore(self.data_dir, fsync=False, snapshot_every=1) as store:
            store.set("k1", "v1")
            store.set("k2", "v2")
            store.set("k3", "v3")
            store.delete("k1")
        with DurableKVStore(self.data_dir, fsync=False) as store:
            with self.assertRaises(KeyError):
                store.get("k1")
            self.assertEqual(store.get("k2"), "v2")
            self.assertEqual(store.get("k3"), "v3")

    def test_snapshot_every_n_ops(self) -> None:
        with DurableKVStore(self.data_dir, fsync=False, snapshot_every=1) as store:
            store.set("a", 1)
            store.set("b", 2)
            self.assertTrue(os.path.exists(os.path.join(self.data_dir, "snapshot.json")))
            with open(os.path.join(self.data_dir, "snapshot.json"), encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertEqual(payload["seq"], 2)
            self.assertEqual(payload["data"], {"a": 1, "b": 2})

    def test_torn_wal_tail_is_ignored(self) -> None:
        with DurableKVStore(self.data_dir, fsync=False) as store:
            store.set("ok", True)
        wal_path = os.path.join(self.data_dir, "wal.log")
        with open(wal_path, "ab") as handle:
            handle.write(struct.pack(HEADER_FORMAT, 100, 0))
            handle.write(b"partial")
        with DurableKVStore(self.data_dir, fsync=False) as store:
            self.assertEqual(store.get("ok"), True)

    def test_corrupt_crc_stops_replay(self) -> None:
        with DurableKVStore(self.data_dir, fsync=False) as store:
            store.set("first", 1)
        wal_path = os.path.join(self.data_dir, "wal.log")
        payload = json.dumps({"seq": 2, "op": "SET", "key": "second", "value": 2}).encode()
        with open(wal_path, "ab") as handle:
            handle.write(struct.pack(HEADER_FORMAT, len(payload), 0xDEADBEEF))
            handle.write(payload)
        with DurableKVStore(self.data_dir, fsync=False) as store:
            self.assertEqual(store.get("first"), 1)
            with self.assertRaises(KeyError):
                store.get("second")

    def test_wal_checksum_roundtrip(self) -> None:
        path = os.path.join(self.data_dir, "standalone.wal")
        record = {"seq": 1, "op": "SET", "key": "x", "value": "y"}
        with WriteAheadLog(path, fsync=False) as wal:
            wal.append(record)
        with WriteAheadLog(path, fsync=False) as wal:
            records = list(wal.replay())
        self.assertEqual(records, [record])
        with open(path, "rb") as handle:
            length, crc = struct.unpack(HEADER_FORMAT, handle.read(8))
            payload = handle.read(length)
        self.assertEqual(zlib.crc32(payload) & 0xFFFFFFFF, crc)


if __name__ == "__main__":
    unittest.main()
