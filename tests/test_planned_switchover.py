from __future__ import annotations

import threading
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from orchestration.demo_runner import DemoRunner, Observation, RunStatus, Scenario, ScenarioRun, Step, StepRunLog
from orchestration.scenario_loader import ScenarioLoadError, load_scenarios_from_directory


class PlannedSwitchoverRunnerTests(unittest.TestCase):
    def test_execute_real_switchover_passes_target_master(self) -> None:
        captured: dict[str, object] = {}

        class FakeCluster:
            pass

        def fake_load_cluster_config(_path):
            return FakeCluster()

        def fake_switchover_master_role(_cluster, target_master=None):
            captured["target_master"] = target_master
            return {
                "success": True,
                "roles": {"master": "pg-node-2", "slave": "pg-node-1"},
                "diagnostics": [],
                "messages": [],
                "downtime_sec": 4.2,
                "orchestration_duration_sec": 8.4,
            }

        fake_module = types.SimpleNamespace(
            load_cluster_config=fake_load_cluster_config,
            switchover_master_role=fake_switchover_master_role,
        )
        runner = DemoRunner([])
        step = Step(
            action_type="switchover",
            target_node="cluster",
            params={"cluster_config_path": "config/cluster.json", "target_master": "pg-node-2"},
        )

        with patch.dict("sys.modules", {"cluster_demo": fake_module}):
            result = runner._execute_real_switchover(step)

        self.assertEqual(captured["target_master"], "pg-node-2")
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

    def test_execute_real_switchover_fails_when_target_not_in_available_slaves(self) -> None:
        class FakeCluster:
            nodes = [object(), object()]

        def fake_load_cluster_config(_path):
            return FakeCluster()

        def fake_get_target_database(_cluster, _mode):
            return "postgres"

        def fake_fetch_all_node_metrics(_nodes, _db):
            return [
                {"node": "pg-node-1", "role": "master", "tx_read_only": "off"},
                {"node": "pg-node-2", "role": "slave", "tx_read_only": "on"},
            ]

        def fake_classify_node_role(role, _tx_read_only):
            return str(role)

        def fake_switchover_master_role(_cluster, target_master=None):  # pragma: no cover
            return {"success": True, "roles": {"master": target_master, "slave": "pg-node-1"}}

        fake_module = types.SimpleNamespace(
            load_cluster_config=fake_load_cluster_config,
            get_target_database=fake_get_target_database,
            fetch_all_node_metrics=fake_fetch_all_node_metrics,
            classify_node_role=fake_classify_node_role,
            switchover_master_role=fake_switchover_master_role,
        )
        runner = DemoRunner([])
        step = Step(
            action_type="switchover",
            target_node="cluster",
            params={"cluster_config_path": "config/cluster.json", "target_master": "pg-node-3"},
        )

        with patch.dict("sys.modules", {"cluster_demo": fake_module}):
            with self.assertRaises(RuntimeError) as raised:
                runner._execute_real_switchover(step)

        self.assertIn("available_slaves", str(raised.exception))

    def test_verify_availability_step_is_single_shot(self) -> None:
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
        runner.wait_until = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("wait_until must not be called"))  # type: ignore[method-assign]
        runner._resolve_observation_source = lambda *_args, **_kwargs: (  # type: ignore[method-assign]
            lambda: Observation(
                timestamp=datetime.now(timezone.utc),
                source="cluster-state",
                metric_event="availability",
                value={"measured_downtime_sec": 5.0, "availability_ratio": 0.95},
            )
        )

        runner._execute_step(run_id, 0, step, cancel_event)
        self.assertEqual(runner._runs[run_id].step_logs[0].status, "succeeded")

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


class LeaderCrashFailoverScenarioValidationTests(unittest.TestCase):
    def test_leader_crash_failover_requires_expected_new_master_param(self) -> None:
        with self.assertRaises(ScenarioLoadError):
            load_scenarios_from_directory("tests/fixtures/scenario_loader/missing_expected_master")


if __name__ == "__main__":
    unittest.main()
