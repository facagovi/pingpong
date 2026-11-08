"""Testes locais do adap.py (timeout adaptativo), sem depender do servidor externo."""

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


CLIENT = Path(__file__).resolve().parents[1] / "adap.py"
RTT_S = 0.01  # atraso de resposta do servidor de teste


def execute(protocol, n_segments, dropped_first=(), loss="0", seed=None,
            extra=(), window="4", run_timeout=30):
    payloads = [bytes([seq % 256]) * 512 for seq in range(n_segments)]
    expected = hashlib.md5(b"".join(payloads)).hexdigest()
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    server.settimeout(0.05)
    requests = collections.Counter()
    acks = collections.Counter()
    stopped = threading.Event()

    def serve():
        while not stopped.is_set():
            try:
                message, address = server.recvfrom(65507)
            except socket.timeout:
                continue
            except OSError:
                break
            if message.startswith(b"HELLO|"):
                server.sendto((f"OK|file_size={512 * n_segments}|checksum={expected}|seed=0|"
                               f"total_segments={n_segments}|segment_size=512").encode(), address)
            elif message.startswith(b"REQ|seq="):
                seq = int(message.split(b"=", 1)[1])
                requests[seq] += 1
                if seq not in dropped_first or requests[seq] > 1:
                    time.sleep(RTT_S / 4)  # atraso pequeno e em ordem
                    server.sendto(f"DATA|seq={seq}|total=512|".encode() + payloads[seq], address)
            elif message.startswith(b"ACK|seq="):
                acks[int(message.split(b"=", 1)[1])] += 1
            elif message.startswith(b"FIN|"):
                checksum = message.split(b"=", 1)[1].decode()
                server.sendto(b"DONE|ok=1" if checksum == expected else b"DONE|ok=0", address)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable]
            if seed is not None:
                command += ["-c",
                            "import random,runpy,sys; random.seed(int(sys.argv[1])); "
                            "path=sys.argv[2]; sys.argv=sys.argv[2:]; "
                            "runpy.run_path(path,run_name='__main__')",
                            str(seed)]
            command += [str(CLIENT), "--server", "127.0.0.1",
                        "--port", str(server.getsockname()[1]), "--group", "teste",
                        "--file", "small", "--protocol", protocol,
                        "--window", window, "--loss", loss, *extra]
            result = subprocess.run(command, cwd=directory, capture_output=True,
                                    text=True, timeout=run_timeout)
            if result.returncode:
                raise AssertionError(result.stdout + result.stderr)
            with next(Path(directory).glob("metrics_*.csv")).open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            with next(Path(directory).glob("summary_*.csv")).open(newline="", encoding="utf-8") as f:
                totals = next(csv.DictReader(f))
            received = next(Path(directory).glob("recebido_*.bin")).read_bytes()
            return rows, totals, requests, acks, received, b"".join(payloads)
    finally:
        stopped.set()
        server.close()
        thread.join(timeout=1)


class AdaptiveTimeoutTests(unittest.TestCase):
    def check_common(self, rows, totals, requests, received, expected_bytes):
        self.assertEqual(received, expected_bytes)
        self.assertEqual(totals["md5_ok"], "1")
        retransmissions = sum(int(row["retransmissions"]) for row in rows)
        self.assertEqual(retransmissions, sum(requests.values()) - len(rows))
        self.assertEqual(int(totals["total_retransmissions"]), retransmissions)
        self.assertEqual(int(totals["total_timeouts"]),
                         sum(int(row["timeout_count"]) for row in rows))
        expected_rate = len(expected_bytes) * 8 / float(totals["total_time_s"]) / 1000
        self.assertAlmostEqual(float(totals["average_throughput_kbps"]), expected_rate, delta=0.01)

    def test_no_loss_converges_and_acks_every_segment(self):
        for protocol in ("gbn", "sr"):
            with self.subTest(protocol=protocol):
                rows, totals, requests, acks, received, data = execute(protocol, 40)
                self.check_common(rows, totals, requests, received, data)
                self.assertEqual(totals["timeout_mode"], "adaptive")
                self.assertEqual(int(totals["total_retransmissions"]), 0)
                self.assertTrue(all(row["rtt_valid"] == "1" for row in rows))
                # depois de algumas amostras o timeout cai bem abaixo de 1 s
                self.assertLess(float(rows[-1]["timeout_ms"]), 200)
                self.assertEqual(set(acks), set(range(40)))

    def test_fixed_mode_keeps_timeout_and_still_records_ewma(self):
        rows, totals, requests, acks, received, data = execute(
            "gbn", 8, dropped_first={3}, extra=("--timeout-fixo", "1000"))
        self.check_common(rows, totals, requests, received, data)
        self.assertEqual(totals["timeout_mode"], "fixed")
        self.assertTrue(all(float(row["timeout_ms"]) == 1000.0 for row in rows))
        self.assertTrue(all(row["rtt_estimated_ms"] != "" for row in rows))
        self.assertGreaterEqual(float(totals["transfer_time_s"]), 1.0)

    def test_adaptive_recovers_fast_and_undoes_backoff(self):
        rows, totals, requests, acks, received, data = execute("gbn", 12, dropped_first={6})
        self.check_common(rows, totals, requests, received, data)
        self.assertEqual(int(rows[6]["timeout_count"]), 1)
        self.assertEqual(rows[6]["rtt_valid"], "0")
        self.assertEqual(rows[6]["rtt_ms"], "")
        # perda recuperada com o timeout adaptativo, bem antes de 1 s
        self.assertLess(float(totals["transfer_time_s"]), 0.8)
        # o backoff aparece no segmento retransmitido e é desfeito depois
        self.assertGreater(float(rows[6]["timeout_ms"]), float(rows[5]["timeout_ms"]))
        self.assertLess(float(rows[-1]["timeout_ms"]), float(rows[6]["timeout_ms"]))

    def test_sr_retransmits_only_the_lost_segment(self):
        rows, totals, requests, acks, received, data = execute("sr", 12, dropped_first={6})
        self.check_common(rows, totals, requests, received, data)
        self.assertEqual([int(row["retransmissions"]) for row in rows],
                         [0] * 6 + [1] + [0] * 5)

    def test_gbn_with_10_percent_loss_does_not_get_stuck_in_backoff(self):
        rows, totals, requests, acks, received, data = execute(
            "gbn", 300, loss="0.10", seed=7, window="16", run_timeout=60)
        self.check_common(rows, totals, requests, received, data)
        self.assertGreater(int(totals["total_timeouts"]), 10)
        self.assertLess(max(float(row["timeout_ms"]) for row in rows), 5000)
        self.assertLess(float(totals["total_time_s"]), 30)


if __name__ == "__main__":
    unittest.main()
