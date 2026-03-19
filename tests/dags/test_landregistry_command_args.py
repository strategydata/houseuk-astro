import importlib.util
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "landregistry_dag_under_test"
MODULE_FILE = REPO_ROOT / "dags" / "extract" / "landregistry_extract.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_module() -> ModuleType:
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_landregistry_dag_stores_bucket_name() -> None:
    module = load_module()
    dag = module.landregistry_extract()
    task = dag.get_task("landregistry_extract_task")

    assert not any(secret.deploy_target == "LANDREGISTRY_API_KEY" for secret in task.secrets)
