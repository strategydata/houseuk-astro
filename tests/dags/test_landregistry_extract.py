"""Test the Land Registry extraction DAG."""

import os

from airflow.models import DagBag


def get_import_errors() -> list[tuple[str | None, str | None]]:
    """Generate tuples for import errors in the DAG bag."""
    dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path: str) -> str:
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    # Prepend "(None, None)" to ensure a test object is created even if it's a no-op.
    return [(None, None)] + [(strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()]
