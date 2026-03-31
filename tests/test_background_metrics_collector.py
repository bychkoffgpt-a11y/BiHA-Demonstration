from __future__ import annotations

import threading
import time
import unittest
from unittest.mock import patch

import cluster_demo
from cluster_demo import BackgroundMetricsCollector, ClusterConfig, NodeConfig


class _FakeWorkloadGenerator:
    def stats_snapshot(self) -> dict[str, int]:
        return {"read_tx": 0, "write_tx": 0, "errors": 0}


class BackgroundMetricsCollectorTests(unittest.TestCase):
    def test_runtime_error_on_shutdown_is_handled_gracefully(self) -> None:
        collector = BackgroundMetricsCollector()
        cluster = ClusterConfig(
            nodes=[NodeConfig(name="n1", dsn="postgresql://localhost/postgres")],
            vip_dsn="postgresql://localhost/postgres",
            poll_interval_sec=0,
        )
        wg = _FakeWorkloadGenerator()
        fetch_started = threading.Event()
        release_fetch = threading.Event()

        def fake_fetch_all_node_metrics(_nodes, _target_db):
            fetch_started.set()
            release_fetch.wait(timeout=1.0)
            if collector._stop_event.is_set():
                raise RuntimeError("cannot schedule new futures after interpreter shutdown")
            return [
                {
                    "node": "n1",
                    "active_locks": 0,
                    "active_queries": 0,
                    "blks_read": 0,
                    "role": "master",
                    "blk_read_time_ms": 0.0,
                    "blk_write_time_ms": 0.0,
                    "disk_io_queue": 0.0,
                    "disk_read_kb_s_os": 0.0,
                    "disk_write_kb_s_os": 0.0,
                    "disk_util_pct_os": 0.0,
                }
            ]

        with (
            patch.object(cluster_demo, "fetch_all_node_metrics", side_effect=fake_fetch_all_node_metrics),
            patch.object(cluster_demo, "LOGGER") as logger_mock,
        ):
            collector.start(cluster, wg, mode="rw")
            self.assertTrue(fetch_started.wait(timeout=1.0))

            stop_thread = threading.Thread(target=collector.stop)
            stop_thread.start()
            release_fetch.set()
            stop_thread.join(timeout=1.0)

            deadline = time.time() + 1.0
            while collector.running and time.time() < deadline:
                time.sleep(0.01)

            self.assertFalse(collector.running)
            logger_mock.exception.assert_not_called()
            logger_mock.info.assert_called()


if __name__ == "__main__":
    unittest.main()
