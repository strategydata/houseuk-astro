import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "epc_annual_dag_under_test"
MODULE_FILE = REPO_ROOT / "dags" / "extract" / "epc_annual_extract.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_module():
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_epc_annual_dag_refreshes_previous_year_with_bulk_command():
    module = load_module()
    dag = module.epc_annual_extract()
    task = dag.get_task("epc_annual_extract_task")
    command = task.arguments[0]

    assert "python extract/epc/src/execute.py bulk" in command
    assert "--start_year {{ data_interval_start.year }}" in command
    assert "--end_year {{ data_interval_start.year }}" in command
