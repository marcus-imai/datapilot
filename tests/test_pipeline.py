"""Tests for the core Pipeline class."""

import pytest
from datapilot import Pipeline
from datapilot.connectors import DictSource


def test_pipeline_empty_source():
    """Pipeline with no source yields nothing."""
    p = Pipeline(name="test-empty")
    assert p.count() == 0


def test_pipeline_basic_iteration():
    """Pipeline iterates over source data."""
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    p = Pipeline(source=DictSource(data), name="test-basic")
    result = p.to_list()
    assert len(result) == 3
    assert result[0]["id"] == 1


def test_pipeline_chain_steps():
    """Pipeline can chain multiple steps."""
    data = [{"v": 1}, {"v": 2}, {"v": 3}]

    def double(rows):
        for r in rows:
            r["v"] = r["v"] * 2
            yield r

    p = Pipeline(source=DictSource(data), name="test-chain")
    p = p | double | double
    result = p.to_list()

    assert result[0]["v"] == 4
    assert result[2]["v"] == 12


def test_pipeline_count():
    """Pipeline count is correct."""
    data = [{"x": i} for i in range(100)]
    p = Pipeline(source=DictSource(data), name="test-count")
    assert p.count() == 100
