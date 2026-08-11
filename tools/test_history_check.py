import unittest

from history_check import HistoryError, check_linearizable, check_snapshot_isolation


class LinearizabilityTests(unittest.TestCase):
    def test_accepts_overlapping_legal_history(self):
        operations = [
            {"op": "put", "key": "x", "input": 1, "output": True, "start": 0, "end": 3},
            {"op": "get", "key": "x", "output": 1, "start": 2, "end": 4},
            {"op": "cas", "key": "x", "input": [1, 2], "output": True, "start": 5, "end": 6},
            {"op": "get", "key": "x", "output": 2, "start": 7, "end": 8},
        ]
        result = check_linearizable(operations)
        self.assertTrue(result.valid)
        self.assertEqual(len(result.order), len(operations))

    def test_rejects_stale_read_after_completed_write(self):
        operations = [
            {"op": "put", "key": "x", "input": 1, "output": True, "start": 0, "end": 1},
            {"op": "get", "key": "x", "output": None, "start": 2, "end": 3},
        ]
        self.assertFalse(check_linearizable(operations).valid)

    def test_rejects_invalid_interval(self):
        with self.assertRaises(HistoryError):
            check_linearizable([{"op": "get", "key": "x", "start": 2, "end": 1}])


class SnapshotTests(unittest.TestCase):
    def test_accepts_repeatable_snapshot(self):
        history = {
            "initial": {"x": 0},
            "transactions": [
                {"id": "writer", "read_ts": 0, "commit_ts": 2, "status": "committed", "writes": {"x": 1}},
                {
                    "id": "reader",
                    "read_ts": 0,
                    "status": "aborted",
                    "writes": {},
                    "reads": [
                        {"key": "x", "value": 0, "version": 0},
                        {"key": "x", "value": 0, "version": 0},
                    ],
                },
            ],
        }
        self.assertTrue(check_snapshot_isolation(history).valid)

    def test_rejects_two_overlapping_committed_writers(self):
        history = {
            "initial": {"x": 0},
            "transactions": [
                {"id": "a", "read_ts": 0, "commit_ts": 1, "status": "committed", "writes": {"x": 1}},
                {"id": "b", "read_ts": 0, "commit_ts": 2, "status": "committed", "writes": {"x": 2}},
            ],
        }
        self.assertFalse(check_snapshot_isolation(history).valid)


if __name__ == "__main__":
    unittest.main()
