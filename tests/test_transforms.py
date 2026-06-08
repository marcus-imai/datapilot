"""Tests for built-in transforms."""

import pytest
from datapilot import Pipeline
from datapilot.connectors import DictSource
from datapilot import transforms as T


def test_filter():
    """Filter keeps matching rows."""
    data = [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}]
    p = Pipeline(source=DictSource(data)) | T.filter(lambda r: r["n"] > 2)
    assert p.to_list() == [{"n": 3}, {"n": 4}]


def test_project():
    """Project keeps only specified fields."""
    data = [{"a": 1, "b": 2, "c": 3}]
    p = Pipeline(source=DictSource(data)) | T.project("a", "c")
    assert p.to_list() == [{"a": 1, "c": 3}]


def test_deduplicate():
    """Deduplicate removes duplicate rows."""
    data = [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}]
    p = Pipeline(source=DictSource(data)) | T.deduplicate(key="id")
    result = p.to_list()
    assert len(result) == 3
    ids = [r["id"] for r in result]
    assert ids == [1, 2, 3]


def test_rename_field():
    """Rename field works correctly."""
    data = [{"old_name": "value"}]
    p = Pipeline(source=DictSource(data)) | T.rename_field("old_name", "new_name")
    assert p.to_list() == [{"new_name": "value"}]


def test_sort_by():
    """Sort by key works correctly."""
    data = [{"v": 3}, {"v": 1}, {"v": 2}]
    p = Pipeline(source=DictSource(data)) | T.sort_by("v")
    result = p.to_list()
    assert [r["v"] for r in result] == [1, 2, 3]


def test_add_field():
    """Add field appends computed field."""
    data = [{"x": 10}]
    p = Pipeline(source=DictSource(data)) | T.add_field("y", lambda r: r["x"] * 2)
    assert p.to_list() == [{"x": 10, "y": 20}]
