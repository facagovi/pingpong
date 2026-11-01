"""Testes locais do timeout e das métricas, sem depender do servidor externo."""

import collections
import csv
import hashlib
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path


CLIENT = Path(__file__).resolve().parents[1] / "gbnsr.py"
PAYLOADS = [bytes([seq]) * 512 for seq in range(4)]
EXPECTED = hashlib.md5(b"".join(PAYLOADS)).hexdigest()


def execute(protocol, dropped_first, loss="0", seed=None):
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    server.settimeout(0.05)
    requests = collections.Counter()
    stopped = threading.Event()
    peer = None
    last_duplicate = 0.0

    def data(seq):
        return f"DATA|seq={seq}|total=512|".encode() + PAYLOADS[seq]

    def serve():
        nonlocal peer, last_duplicate
        while not stopped.is_set():
            try:
                message, address = server.recvfrom(65507)
            except socket.timeout:
                # Pacotes alheios ao REQ pendente não podem adiar seu timeout.
                if peer and requests[3] and requests[0] == 1:
                    now = time.monotonic()
                    if now - last_duplicate >= 0.1:
                        server.sendto(data(3), peer)
                        last_duplicate = now
                continue
            except OSError:
                break
            peer = address
            if message.startswith(b"HELLO|"):
                response = (
                    f"OK|file_size=2048|checksum={EXPECTED}|seed=0|"
                    "total_segments=4|segment_size=512"
                ).encode()
                server.sendto(response, address)
            elif message.startswith(b"REQ|seq="):
                seq = int(message.split(b"=", 1)[1])
                requests[seq] += 1
                if seq not in dropped_first or requests[seq] > 1:
                    server.sendto(data(seq), address)
            elif message.startswith(b"FIN|"):
                checksum = message.split(b"=", 1)[1].decode()
                server.sendto(b"DONE|ok=1" if checksum == EXPECTED else b"DONE|ok=0", address)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable]
            if seed is not None:
                command += [
                    "-c",
                    "import random,runpy,sys; random.seed(int(sys.argv[1])); "
                    "path=sys.argv[2]; sys.argv=sys.argv[2:]; "
                    "runpy.run_path(path,run_name='__main__')",
                    str(seed),
                ]
            command += [
                str(CLIENT), "--server", "127.0.0.1",
                "--port", str(server.getsockname()[1]), "--group", "teste",
                "--file", "small", "--protocol", protocol,
                "--window", "4", "--loss", loss,
            ]
            result = subprocess.run(command, cwd=directory, capture_output=True,
                                    text=True, timeout=6)
            if result.returncode:
                raise AssertionError(result.stdout + result.stderr)
            metrics = next(Path(directory).glob("metrics_*.csv"))
            summary = next(Path(directory).glob("summary_*.csv"))
            with metrics.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            with summary.open(newline="", encoding="utf-8") as f:
                totals = next(csv.DictReader(f))
            received = (Path(directory) / "recebido_small.bin").read_bytes()
            return rows, totals, requests, received
    finally:
        stopped.set()
        server.close()
        thread.join(timeout=1)


class TimeoutMetricsTests(unittest.TestCase):
    def check_summary(self, rows, totals, requests, received):
        self.assertEqual(received, b"".join(PAYLOADS))
        retransmissions = sum(int(row["retransmissions"]) for row in rows)
        timeouts = sum(int(row["timeout_count"]) for row in rows)
        self.assertEqual(retransmissions, sum(requests.values()) - 4)
        self.assertEqual(int(totals["total_retransmissions"]), retransmissions)
        self.assertEqual(int(totals["total_timeouts"]), timeouts)
        self.assertGreaterEqual(float(totals["total_time_s"]),
                                float(totals["transfer_time_s"]))
        expected_rate = 2048 * 8 / float(totals["total_time_s"]) / 1000
        self.assertAlmostEqual(float(totals["average_throughput_kbps"]),
                               expected_rate, delta=0.01)

    def test_gbn_retransmits_whole_pending_window_on_base_deadline(self):
        rows, totals, requests, received = execute("gbn", {0})
        self.check_summary(rows, totals, requests, received)
        self.assertEqual([int(row["retransmissions"]) for row in rows], [1, 1, 1, 1])
        self.assertEqual([int(row["timeout_count"]) for row in rows], [1, 0, 0, 0])
        self.assertEqual(rows[0]["rtt_valid"], "0")
        self.assertEqual(rows[0]["rtt_ms"], "")
        self.assertGreaterEqual(float(rows[0]["segment_elapsed_ms"]), 1000)

    def test_no_loss_keeps_rtt_valid_for_all_protocols(self):
        for protocol in ("saw", "gbn", "sr"):
            with self.subTest(protocol=protocol):
                rows, totals, requests, received = execute(protocol, set())
                self.check_summary(rows, totals, requests, received)
                self.assertEqual([int(row["retransmissions"]) for row in rows],
                                 [0, 0, 0, 0])
                self.assertEqual([row["rtt_valid"] for row in rows],
                                 ["1", "1", "1", "1"])
                self.assertTrue(all(float(row["rtt_ms"]) > 0 for row in rows))

    def test_saw_counts_real_loss_after_timeout(self):
        rows, totals, requests, received = execute("saw", {0})
        self.check_summary(rows, totals, requests, received)
        self.assertEqual([int(row["retransmissions"]) for row in rows],
                         [1, 0, 0, 0])
        self.assertEqual([int(row["timeout_count"]) for row in rows],
                         [1, 0, 0, 0])
        self.assertEqual(rows[0]["rtt_valid"], "0")
        self.assertEqual(rows[0]["rtt_ms"], "")
        self.assertGreaterEqual(float(rows[0]["segment_elapsed_ms"]), 1000)

    def test_saw_simulated_loss_waits_for_timeout_and_counts_retry(self):
        rows, totals, requests, received = execute("saw", set(), loss="0.10", seed=31)
        self.check_summary(rows, totals, requests, received)
        self.assertEqual([int(row["simulated_loss"]) for row in rows],
                         [1, 0, 0, 0])
        self.assertEqual([int(row["retransmissions"]) for row in rows],
                         [1, 0, 0, 0])
        self.assertEqual([int(row["timeout_count"]) for row in rows],
                         [1, 0, 0, 0])
        self.assertGreaterEqual(float(rows[0]["segment_elapsed_ms"]), 1000)

    def test_saw_counts_repeated_simulated_drops_of_same_segment(self):
        rows, totals, requests, received = execute("saw", set(), loss="0.10", seed=123)
        self.check_summary(rows, totals, requests, received)
        self.assertEqual(rows[0]["simulated_loss"], "1")
        self.assertEqual(int(rows[0]["retransmissions"]), 2)
        self.assertEqual(int(rows[0]["timeout_count"]), 2)
        self.assertEqual(requests[0], 3)

    def test_sr_retransmits_each_expired_request_only(self):
        rows, totals, requests, received = execute("sr", {0, 1})
        self.check_summary(rows, totals, requests, received)
        self.assertEqual([int(row["retransmissions"]) for row in rows], [1, 1, 0, 0])
        self.assertEqual([int(row["timeout_count"]) for row in rows], [1, 1, 0, 0])
        self.assertEqual([row["rtt_valid"] for row in rows], ["0", "0", "1", "1"])
        self.assertGreaterEqual(float(rows[0]["segment_elapsed_ms"]), 1000)


if __name__ == "__main__":
    unittest.main()
