"""Built-in transform functions for data pipelines."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any, Callable, Iterator


def filter(predicate: Callable[[dict], bool]) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Keep only rows where predicate(row) is True."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        for row in rows:
            if predicate(row):
                yield row

    apply.__name__ = f"filter({predicate.__name__})"
    return apply


def map_fn(transform: Callable[[dict], dict]) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Apply a transformation function to each row."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        for row in rows:
            yield transform(row)

    apply.__name__ = f"map({transform.__name__})"
    return apply


def project(*fields: str) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Keep only the specified fields from each row."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        for row in rows:
            yield {k: row.get(k) for k in fields if k in row}

    apply.__name__ = f"project({','.join(fields)})"
    return apply


def flatten(field: str, separator: str = ".") -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Flatten a nested dict field into top-level keys."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        for row in rows:
            if field in row and isinstance(row[field], dict):
                flat = {k: v for k, v in row.items() if k != field}
                flat.update({f"{field}{separator}{k}": v for k, v in row[field].items()})
                yield flat
            else:
                yield row

    apply.__name__ = f"flatten({field})"
    return apply


def deduplicate(key: str | None = None) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Remove duplicate rows, optionally deduplicating by a specific key."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        seen: set[Any] = set()
        for row in rows:
            dedup_key = (key, row.get(key) if key else json.dumps(row, sort_keys=True))
            if dedup_key not in seen:
                seen.add(dedup_key)
                yield row

    apply.__name__ = f"deduplicate(key={key})"
    return apply


def group_by(*keys: str) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Group rows by the specified keys."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        groups: dict[tuple, list[dict]] = defaultdict(list)
        for row in rows:
            group_key = tuple(row.get(k) for k in keys)
            groups[group_key].append(row)
        for group_key, group_rows in groups.items():
            result = dict(zip(keys, group_key))
            result["_group"] = group_rows
            yield result

    apply.__name__ = f"group_by({','.join(keys)})"
    return apply


def aggregate(
    specs: dict[str, tuple[str, str]]
) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Compute aggregates over grouped rows.

    Args:
        specs: { "output_field": ("input_field", "agg_func") }
               agg_func: sum, count, min, max, avg, first, last, collect
    """

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        for row in rows:
            group = row.get("_group", [row])
            result = {k: v for k, v in row.items() if k != "_group"}
            for out_field, (in_field, agg_func) in specs.items():
                values = [r.get(in_field, 0) for r in group if in_field in r]
                if agg_func == "sum":
                    result[out_field] = sum(values)
                elif agg_func == "count":
                    result[out_field] = len(values)
                elif agg_func == "min":
                    result[out_field] = min(values) if values else None
                elif agg_func == "max":
                    result[out_field] = max(values) if values else None
                elif agg_func == "avg":
                    result[out_field] = sum(values) / len(values) if values else None
                elif agg_func == "first":
                    result[out_field] = values[0] if values else None
                elif agg_func == "last":
                    result[out_field] = values[-1] if values else None
                elif agg_func == "collect":
                    result[out_field] = values
            yield result

    apply.__name__ = f"aggregate({list(specs.keys())})"
    return apply


def add_field(name: str, value: Any | Callable[[dict], Any]) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Add a computed field to each row."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        for row in rows:
            row[name] = value(row) if callable(value) else value
            yield row

    apply.__name__ = f"add_field({name})"
    return apply


def rename_field(old: str, new: str) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Rename a field in each row."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        for row in rows:
            if old in row:
                row[new] = row.pop(old)
            yield row

    apply.__name__ = f"rename({old}->{new})"
    return apply


def sort_by(*keys: str, reverse: bool = False) -> Callable[[Iterator[dict]], Iterator[dict]]:
    """Sort rows by the specified keys."""

    def apply(rows: Iterator[dict]) -> Iterator[dict]:
        data = list(rows)
        data.sort(key=lambda r: tuple(r.get(k) for k in keys), reverse=reverse)
        yield from data

    apply.__name__ = f"sort_by({','.join(keys)})"
    return apply