from __future__ import annotations

import threading
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from orchestration.demo_runner import (
    CURRENT_STANDBY_TARGET_MARKER,
    DemoRunner,
    Observation,
    RunStatus,
    Scenario,
    ScenarioRun,
    Step,
    StepRunLog,
)
from orchestration.scenario_loader import load_scenarios_from_directory


class PlannedSwitchoverRunnerTests(unittest.TestCase):
    def test_resolve_standby_target_uses_params_override(self) -> None:
        runner = DemoRunner([])
        step = Step(
            action_type="stop_db_service",
            target_node=CURRENT_STANDBY_TARGET_MARKER,
            params={"target_standby": "pg-node-9"},
        )

        resolved = runner._resolve_action_target_node(step)

        self.assertEqual(resolved, "pg-node-9")

    def test_resolve_standby_target_fails_without_cluster_config_or_param(self) -> None:
        runner = DemoRunner([])
        step = Step(
            action_type="stop_db_service",
            target_node=CURRENT_STANDBY_TARGET_MARKER,
            params={},
        )

        with self.assertRaises(RuntimeError) as raised:
            runner._resolve_action_target_node(step)

        self.assertIn("standby target cannot be resolved", str(raised.exception))

    def test_resolve_standby_target_fails_when_cluster_has_no_standby(self) -> None:
        class FakeCluster:
            nodes = [object()]

        def fake_load_cluster_config(_path):
            return FakeCluster()

        def fake_get_target_database(_cluster, _mode):
            return "postgres"

        def fake_fetch_all_node_metrics(_nodes, _db):
            return [{"node": "pg-node-1", "role": "master", "tx_read_only": "off"}]

        def fake_classify_node_role(role, _tx_read_only):
            return str(role)

        fake_module = types.SimpleNamespace(
            classify_node_role=fake_classify_node_role,
            fetch_all_node_metrics=fake_fetch_all_node_metrics,
            get_target_database=fake_get_target_database,
            load_cluster_config=fake_load_cluster_config,
        )
        runner = DemoRunner([])
        step = Step(
            action_type="stop_db_service",
            target_node=CURRENT_STANDBY_TARGET_MARKER,
            params={"cluster_config_path": "config/cluster.json"},
        )

        with patch.dict("sys.modules", {"cluster_demo": fake_module}):
            with self.assertRaises(RuntimeError) as raised:
                runner._resolve_action_target_node(step)

        self.assertIn("No standby nodes detected", str(raised.exception))

    @patch("orchestration.demo_runner.subprocess.run")
    def test_execute_orchestration_action_uses_cli_backend(self, run_mock) -> None:
        run_mock.return_value.returncode = 0
        run_mock.return_value.stdout = (
            '{"roles":{"master":"pg-node-2","slave":"pg-node-1"},'
            '"downtime_sec":4.2,"orchestration_duration_sec":8.4,"target_master":"pg-node-2"}'
        )
        run_mock.return_value.stderr = ""
        runner = DemoRunner([])
        step = Step(
            action_type="switchover",
            target_node="cluster",
            params={"cluster_config_path": "config/cluster.json", "target_master": "pg-node-2"},
        )

        result = runner._execute_orchestration_action(step, "switchover")

        run_mock.assert_called_once()
        called_command = run_mock.call_args.args[0]
        self.assertIn("--target-master", called_command)
        self.assertEqual(result["orchestration"]["target_master"], "pg-node-2")
        self.assertEqual(result["orchestration"]["downtime_sec"], 4.2)
        self.assertEqual(result["orchestration"]["switchover_duration_sec"], 8.4)

    def test_observe_availability_uses_downtime_metric(self) -> None:
        runner = DemoRunner([])
        run_id = "run-1"
        runner._run_metrics[run_id] = {
            "last_switchover_duration_sec": 31.5,
            "last_switchover_downtime_sec": 10.0,
        }
        runner._fetch_cluster_state = lambda _step: {
            "cluster_config_path": "config/cluster.json",
            "rows": [{"node": "pg-node-1", "status": "up"}],
        }
        step = Step(
            action_type="verify_availability",
            target_node="cluster",
            params={"cluster_config_path": "config/cluster.json", "slo_window_sec": 120},
        )

        observation = runner._observe_availability(run_id, step)

        self.assertEqual(observation.value["measured_downtime_sec"], 10.0)
        self.assertAlmostEqual(observation.value["availability_ratio"], 0.916667, places=6)

    def test_apply_params_override_replaces_required_placeholder(self) -> None:
        runner = DemoRunner([])
        scenario = runner._apply_params_override(
            scenario=Scenario(
                id="planned_switchover",
                name="Planned switchover",
                description="desc",
                success_criteria="ok",
                steps=[
                    Step(
                        action_type="switchover",
                        target_node="cluster",
                        params={"target_master": "__REQUIRED_TARGET_MASTER__"},
                        wait_condition={"current_roles": {"master": {"equals": "__REQUIRED_TARGET_MASTER__"}}},
                        expected={"current_roles": {"master": {"equals": "__REQUIRED_TARGET_MASTER__"}}},
                    )
                ],
            ),
            params_override={"target_master": "pg-node-2"},
        )
        step = scenario.steps[0]
        self.assertEqual(step.params["target_master"], "pg-node-2")
        self.assertEqual(step.wait_condition["current_roles"]["master"]["equals"], "pg-node-2")
        self.assertEqual(step.expected["current_roles"]["master"]["equals"], "pg-node-2")

    @patch("orchestration.demo_runner.subprocess.run")
    def test_execute_orchestration_action_raises_on_cli_failure(self, run_mock) -> None:
        run_mock.return_value.returncode = 7
        run_mock.return_value.stdout = ""
        run_mock.return_value.stderr = "boom"
        runner = DemoRunner([])
        step = Step(
            action_type="verify_roles",
            target_node="cluster",
            params={"cluster_config_path": "config/cluster.json"},
        )

        with self.assertRaises(RuntimeError) as raised:
            runner._execute_orchestration_action(step, "verify_roles")

        self.assertIn("exit_code=7", str(raised.exception))

    def test_verify_availability_step_uses_wait_until(self) -> None:
        runner = DemoRunner([])
        step = Step(
            action_type="verify_availability",
            target_node="cluster",
            params={"cluster_config_path": "config/cluster.json"},
            wait_condition={},
            expected=None,
        )
        run_id = "run-2"
        runner._runs[run_id] = ScenarioRun(
            run_id=run_id,
            scenario_id="planned_switchover",
            scenario_name="Planned switchover",
            status=RunStatus.RUNNING,
            created_at=datetime.now(timezone.utc),
            step_logs=[
                StepRunLog(
                    index=0,
                    action_type=step.action_type,
                    target_node=step.target_node,
                    timeout=step.timeout,
                    expected_result=step.expected,
                )
            ],
        )
        cancel_event = threading.Event()

        runner.execute_action = lambda _step: {"orchestration": {"switchover_duration_sec": 1.0}}  # type: ignore[method-assign]
        runner._record_run_metrics = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
        runner._resolve_observation_source = lambda *_args, **_kwargs: (  # type: ignore[method-assign]
            lambda: Observation(
                timestamp=datetime.now(timezone.utc),
                source="cluster-state",
                metric_event="availability",
                value={"measured_downtime_sec": 5.0, "availability_ratio": 0.95},
            )
        )
        wait_calls = {"count": 0}

        def fake_wait_until(*_args, **_kwargs):
            wait_calls["count"] += 1
            return Observation(
                timestamp=datetime.now(timezone.utc),
                source="cluster-state",
                metric_event="availability",
                value={"measured_downtime_sec": 5.0, "availability_ratio": 0.95},
            )

        runner.wait_until = fake_wait_until  # type: ignore[method-assign]

        runner._execute_step(run_id, 0, step, cancel_event)
        self.assertEqual(runner._runs[run_id].step_logs[0].status, "succeeded")
        self.assertEqual(wait_calls["count"], 1)

    def test_matches_condition_supports_not_equals_run_metric_reference(self) -> None:
        runner = DemoRunner([])
        run_id = "run-metrics"
        runner._run_metrics[run_id] = {"old_master": "pg-node-1"}

        is_match = runner._matches_condition(
            {"current_roles": {"master": "pg-node-2"}},
            {"current_roles": {"master": {"not_equals": "__RUN_METRIC:old_master__"}}},
            run_id=run_id,
        )

        self.assertTrue(is_match)

    def test_record_observation_runtime_context_captures_current_master(self) -> None:
        runner = DemoRunner([])
        run_id = "run-context"
        step = Step(
            action_type="verify_roles",
            target_node="cluster",
            params={"step_name": "capture_initial_master", "store_current_master_as": "old_master"},
        )
        observation = Observation(
            timestamp=datetime.now(timezone.utc),
            source="cluster-state",
            metric_event="roles",
            value={"current_roles": {"master": "pg-node-1"}},
        )

        runner._record_observation_runtime_context(run_id, step, observation)

        self.assertEqual(runner._run_metrics[run_id]["old_master"], "pg-node-1")
        self.assertEqual(
            runner._run_metrics[run_id]["observations_by_step"]["capture_initial_master"]["current_roles"]["master"],
            "pg-node-1",
        )

    def test_matches_condition_supports_count_operators(self) -> None:
        runner = DemoRunner([])

        is_match = runner._matches_condition(
            {"current_roles": {"masters": ["pg-node-2"], "slaves": ["pg-node-1"]}},
            {
                "current_roles": {
                    "masters": {"count_equals": 1},
                    "slaves": {"count_gte": 1, "count_lte": 2},
                }
            },
        )

        self.assertTrue(is_match)

    def test_observe_roles_exposes_role_cardinality_flags(self) -> None:
        def fake_classify_node_role(role, _tx_read_only):
            return str(role)

        fake_module = types.SimpleNamespace(classify_node_role=fake_classify_node_role)
        runner = DemoRunner([])
        runner._fetch_cluster_state = lambda _step: {
            "cluster_config_path": "config/cluster.json",
            "rows": [
                {"node": "pg-node-1", "status": "up", "role": "slave", "tx_read_only": "on", "replication_lag_sec": 0.2},
                {"node": "pg-node-2", "status": "up", "role": "master", "tx_read_only": "off", "replication_lag_sec": 0.0},
            ],
        }
        step = Step(
            action_type="verify_roles",
            target_node="cluster",
            params={"cluster_config_path": "config/cluster.json"},
        )

        with patch.dict("sys.modules", {"cluster_demo": fake_module}):
            observation = runner._observe_roles(step)

        current_roles = observation.value["current_roles"]
        self.assertEqual(current_roles["master_count"], 1)
        self.assertEqual(current_roles["slave_count"], 1)
        self.assertTrue(current_roles["has_single_master"])
        self.assertTrue(current_roles["all_other_nodes_are_slaves"])
        self.assertTrue(current_roles["cardinality_ok"])


class LeaderCrashFailoverScenarioValidationTests(unittest.TestCase):
    def test_leader_crash_failover_allows_missing_expected_new_master_param(self) -> None:
        scenarios = load_scenarios_from_directory("tests/fixtures/scenario_loader/missing_expected_master")

        self.assertEqual(len(scenarios), 1)
        self.assertEqual(scenarios[0].id, "leader_crash_failover")


if __name__ == "__main__":
    unittest.main()
