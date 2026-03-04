import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "crime_dag_under_test"
MODULE_FILE = REPO_ROOT / "dags" / "extract" / "crime_extract.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_module():
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_crime_dag_uses_current_execute_path():
    module = load_module()
    dag = module.crime_extract()
    task = dag.get_task("crime_extract_task")
    command = task.arguments[0]

    assert f"python {module.CRIME_EXECUTE_PATH}" in command
    assert "extract/crime/src/execute.py" not in command
