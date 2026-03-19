"""Tests for Kubernetes helper utilities."""

from __future__ import annotations

import pytest

from include import kubernetes_helpers


def test_get_toleration_with_value() -> None:
    """Return a toleration for the supplied value."""
    tolerations = kubernetes_helpers.get_toleration_with_value("extraction")
    assert tolerations == [
        {"key": "extraction", "operator": "Equal", "value": "true", "effect": "NoSchedule"},
    ]


def test_is_local_test(monkeypatch: pytest.MonkeyPatch) -> None:
    """Detect local testing namespace."""
    monkeypatch.setenv("NAMESPACE", "testing")
    assert kubernetes_helpers.is_local_test() is True


def test_get_affinity_prefers_local_test(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return test affinity when in local testing namespace."""
    monkeypatch.setenv("NAMESPACE", "testing")
    assert kubernetes_helpers.get_affinity("extraction") == kubernetes_helpers.test_affinity


def test_get_affinity_by_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return affinity matching the requested type when not local."""
    monkeypatch.delenv("NAMESPACE", raising=False)
    assert kubernetes_helpers.get_affinity("extraction") == kubernetes_helpers.extraction_affinity
    assert kubernetes_helpers.get_affinity("dbt") == kubernetes_helpers.dbt_affinity
    assert kubernetes_helpers.get_affinity("other") == kubernetes_helpers.production_affinity


def test_get_toleration_by_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return tolerations matching the requested type when not local."""
    monkeypatch.delenv("NAMESPACE", raising=False)
    assert kubernetes_helpers.get_toleration("extraction") == kubernetes_helpers.extraction_tolerations
    assert kubernetes_helpers.get_toleration("dbt") == kubernetes_helpers.dbt_tolerations
    assert kubernetes_helpers.get_toleration("other") == kubernetes_helpers.production_tolerations
