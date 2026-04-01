from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from cluster_probe.cli import ProbeRuntimeError, main, run_probe


class ClusterProbeTests(unittest.TestCase):
    @staticmethod
    def _deps(rows=None, switchover_payload=None):
        if rows is None:
            rows = []
        if switchover_payload is None:
            switchover_payload = {"success": True, "roles": {"master": "pg-node-2", "slave": "pg-node-1"}}

        class FakeCluster:
            nodes = [object(), object()]

        def load_cluster_config(_path):
            return FakeCluster()

        def get_target_database(_cluster, _mode):
            return "postgres"

        def fetch_all_node_metrics(_nodes, _target_db):
            return rows

        def classify_node_role(role, tx_read_only):
            role_text = str(role).lower()
            if role_text == "primary":
                return "master"
            if role_text == "replica":
                return "slave"
            return "unknown"

        def switchover_master_role(_cluster, target_master=None):
            payload = dict(switchover_payload)
            payload.setdefault("target_master", target_master)
            return payload

        return {
            "load_cluster_config": load_cluster_config,
            "get_target_database": get_target_database,
            "fetch_all_node_metrics": fetch_all_node_metrics,
            "classify_node_role": classify_node_role,
            "switchover_master_role": switchover_master_role,
        }

    @patch("cluster_probe.cli._load_cluster_dependencies")
    def test_check_cluster_health_payload(self, deps_mock) -> None:
        deps_mock.return_value = self._deps(
            rows=[
                {"node": "pg-node-1", "status": "up", "role": "primary", "tx_read_only": "off"},
                {"node": "pg-node-2", "status": "up", "role": "replica", "tx_read_only": "on"},
            ]
        )

        payload = run_probe("check_cluster_health", cluster_config_path="config/cluster.json")

        self.assertTrue(payload["all_nodes_up"])
        self.assertEqual(payload["current_roles"]["master"], "pg-node-1")
        self.assertEqual(payload["current_roles"]["slave"], "pg-node-2")

    @patch("cluster_probe.cli._load_cluster_dependencies")
    def test_verify_roles_payload(self, deps_mock) -> None:
        deps_mock.return_value = self._deps(
            rows=[
                {"node": "pg-node-1", "status": "up", "role": "primary", "tx_read_only": "off"},
                {"node": "pg-node-2", "status": "up", "role": "replica", "tx_read_only": "on"},
                {"node": "pg-node-3", "status": "up", "role": "replica", "tx_read_only": "on"},
            ]
        )

        payload = run_probe("verify_roles", cluster_config_path="config/cluster.json")

        current_roles = payload["current_roles"]
        self.assertTrue(current_roles["has_single_master"])
        self.assertEqual(current_roles["master_count"], 1)
        self.assertEqual(current_roles["slave_count"], 2)

    @patch("cluster_probe.cli._load_cluster_dependencies")
    def test_verify_availability_defaults(self, deps_mock) -> None:
        deps_mock.return_value = self._deps(
            rows=[
                {"node": "pg-node-1", "status": "up", "role": "primary", "tx_read_only": "off"},
                {"node": "pg-node-2", "status": "up", "role": "replica", "tx_read_only": "on"},
            ]
        )

        payload = run_probe("verify_availability", cluster_config_path="config/cluster.json", slo_window_sec=120.0)

        self.assertEqual(payload["measured_downtime_sec"], 0.0)
        self.assertEqual(payload["availability_ratio"], 1.0)

    @patch("cluster_probe.cli._load_cluster_dependencies")
    def test_switchover_failure_raises(self, deps_mock) -> None:
        deps_mock.return_value = self._deps(
            switchover_payload={"success": False, "messages": [["error", "failed"]]}
        )

        with self.assertRaises(ProbeRuntimeError):
            run_probe("switchover", cluster_config_path="config/cluster.json", target_master="pg-node-2")

    @patch("cluster_probe.cli.run_probe", side_effect=ProbeRuntimeError("boom"))
    def test_main_returns_one_on_probe_error(self, _run_probe_mock) -> None:
        with patch("sys.stderr"):
            rc = main(["check_cluster_health", "--cluster-config", "config/cluster.json"])

        self.assertEqual(rc, 1)

    @patch("cluster_probe.cli.run_probe", return_value={"success": True, "roles": {"master": "pg-node-2"}})
    def test_main_prints_json(self, _run_probe_mock) -> None:
        with patch("builtins.print") as print_mock:
            rc = main(["switchover", "--cluster-config", "config/cluster.json", "--target-master", "pg-node-2"])

        self.assertEqual(rc, 0)
        rendered = print_mock.call_args.args[0]
        self.assertIsInstance(json.loads(rendered), dict)


if __name__ == "__main__":
    unittest.main()
