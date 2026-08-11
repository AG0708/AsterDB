import json
import socket
import struct
import unittest
import zlib

import benchmark


class BenchmarkHarnessTests(unittest.TestCase):
    def test_request_frame_has_protocol_header_and_checksum(self) -> None:
        frame = benchmark.encode_request(
            {"request_id": 7, "session": None, "operation": "Ping"}
        )
        magic, version, kind, flags, length, checksum = benchmark.HEADER.unpack(
            frame[: benchmark.HEADER.size]
        )
        body = frame[benchmark.HEADER.size :]
        self.assertEqual((magic, version, kind, flags), (b"ASDB", 1, 1, 0))
        self.assertEqual(length, len(body))
        self.assertEqual(checksum, zlib.crc32(body) & 0xFFFFFFFF)
        self.assertEqual(json.loads(body)["request_id"], 7)

    def test_response_decoder_rejects_checksum_damage(self) -> None:
        left, right = socket.socketpair()
        try:
            body = b'{"request_id":1,"result":"Pong"}'
            right.sendall(
                struct.pack(">4sHBBII", b"ASDB", 1, 2, 0, len(body), 0) + body
            )
            with self.assertRaises(benchmark.BenchmarkError):
                benchmark.decode_response(left)
        finally:
            left.close()
            right.close()

    def test_nearest_rank_percentiles_and_empty_distribution(self) -> None:
        values = [1, 2, 3, 4, 100]
        self.assertEqual(benchmark.percentile(values, 0.50), 3)
        self.assertEqual(benchmark.percentile(values, 0.99), 100)
        self.assertIsNone(benchmark.distribution([])["p50_ns"])


if __name__ == "__main__":
    unittest.main()
