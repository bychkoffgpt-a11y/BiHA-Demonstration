from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_DIR = REPO_ROOT / "app"
sys.path.insert(0, str(APP_DIR))


def load_module(module_name: str, relative_path: str):
    module_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Не удалось загрузить модуль {module_name} из {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


dashboard = load_module("cluster_load_dashboard_page", "app/pages/6_cluster_load_dashboard.py")


class DashboardHelpersTest(unittest.TestCase):
    def test_extract_dbname_from_url_dsn(self) -> None:
        self.assertEqual(
            dashboard.extract_dbname_from_dsn("postgresql://postgres:postgres@10.0.0.11:5432/demo_db"),
            "demo_db",
        )

    def test_extract_dbname_from_query_dsn(self) -> None:
        self.assertEqual(
            dashboard.extract_dbname_from_dsn("host=10.0.0.11 port=5432 user=postgres dbname=demo_db"),
            "demo_db",
        )

    def test_fetch_disk_io_uses_partition_prefix_when_exact_device_is_missing(self) -> None:
        node = dashboard.NodeConfig(
            name="pg-master",
            dsn="postgresql://postgres:postgres@127.0.0.1:5432/postgres",
            control_via_ssh=True,
            ssh_host="127.0.0.1",
            disk_device="/dev/nvme0n1",
        )
        iostat_output = """
Linux 6.6.0 (db-host)

Device            r/s     rkB/s   rrqm/s  %rrqm r_await rareq-sz     w/s     wkB/s   wrqm/s  %wrqm w_await wareq-sz aqu-sz  %util
nvme0n1p1        3.00    120.00     0.00   0.00    0.80    40.00   15.00    640.00     0.00   0.00    1.20    42.67   0.02   3.50
""".strip()

        with patch.object(dashboard, "run_ssh_metric", return_value=iostat_output):
            metrics = dashboard.fetch_disk_io(node)

        self.assertEqual(metrics["read_kb_s"], 120.0)
        self.assertEqual(metrics["write_kb_s"], 640.0)


if __name__ == "__main__":
    unittest.main()
