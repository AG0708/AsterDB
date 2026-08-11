import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from cluster_history import (
    HistoryFailure,
    execute_mutation_with_retry,
    stable_client_id,
)


class MutationRetryTests(unittest.TestCase):
    def setUp(self):
        self.nodes = {1: SimpleNamespace(client_address="127.0.0.1:1")}

    def test_known_write_conflict_advances_sequence(self):
        response = {"Query": {"affected_rows": 1, "applied_index": 9}}
        with patch("cluster_history.find_leader", return_value=self.nodes[1]), patch(
            "cluster_history.run_cli",
            side_effect=[
                HistoryFailure("transaction aborted: WriteConflict { key: 1 }"),
                response,
            ],
        ) as run_cli, patch("cluster_history.time.sleep"):
            actual, sequence, conflicts = execute_mutation_with_retry(
                self.nodes,
                "UPDATE t SET value = 2 WHERE id = 1",
                "00" * 15 + "01",
                7,
            )

        self.assertEqual(actual, response)
        self.assertEqual(sequence, 8)
        self.assertEqual(conflicts, 1)
        self.assertEqual(
            [call.kwargs["sequence"] for call in run_cli.call_args_list], [7, 8]
        )

    def test_ambiguous_timeout_reuses_sequence(self):
        response = {"Query": {"affected_rows": 1, "applied_index": 9}}
        with patch("cluster_history.find_leader", return_value=self.nodes[1]), patch(
            "cluster_history.run_cli",
            side_effect=[subprocess.TimeoutExpired("aster-cli", 2), response],
        ) as run_cli, patch("cluster_history.time.sleep"):
            actual, sequence, conflicts = execute_mutation_with_retry(
                self.nodes,
                "UPDATE t SET value = 2 WHERE id = 1",
                "00" * 15 + "01",
                7,
            )

        self.assertEqual(actual, response)
        self.assertEqual(sequence, 7)
        self.assertEqual(conflicts, 0)
        self.assertEqual(
            [call.kwargs["sequence"] for call in run_cli.call_args_list], [7, 7]
        )

    def test_client_identity_is_stable_and_fixed_width(self):
        first = stable_client_id(0x1234, 2)
        self.assertEqual(first, stable_client_id(0x1234, 2))
        self.assertEqual(len(first), 32)
        self.assertNotEqual(first, stable_client_id(0x1234, 3))


if __name__ == "__main__":
    unittest.main()
