from __future__ import annotations

import threading
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from orchestration.demo_runner import DemoRunner, Observation, RunStatus, ScenarioRun, Step, StepRunLog


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


if __name__ == "__main__":
    unittest.main()
