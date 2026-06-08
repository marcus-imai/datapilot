"""Pluggable source and sink connectors."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Union

import fsspec
import pandas as pd


class CSVSource:
    """Read a CSV file as a pipeline source."""

    def __init__(self, path: str | Path, **csv_kwargs):
        self.path = Path(path) if isinstance(path, str) else path
        self.csv_kwargs = csv_kwargs

    def __iter__(self) -> Iterator[dict]:
        with open(self.path, newline="", **self.csv_kwargs) as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row


class JSONSource:
    """Read a JSON / JSON Lines file as a pipeline source."""

    def __init__(self, path: str | Path, lines: bool = False):
        self.path = Path(path) if isinstance(path, str) else path
        self.lines = lines

    def __iter__(self) -> Iterator[dict]:
        with open(self.path) as f:
            if self.lines:
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
            else:
                data = json.load(f)
                if isinstance(data, list):
                    yield from data
                else:
                    yield data


class ParquetSource:
    """Read a Parquet file as a pipeline source."""

    def __init__(self, path: str | Path, columns: list[str] | None = None):
        self.path = Path(path) if isinstance(path, str) else path
        self.columns = columns

    def __iter__(self) -> Iterator[dict]:
        df = pd.read_parquet(self.path, columns=self.columns)
        for row in df.to_dict(orient="records"):
            yield row


class DictSource:
    """In-memory dict or list of dicts as a pipeline source."""

    def __init__(self, data: Union[dict, list[dict]]):
        self.data = data if isinstance(data, list) else [data]

    def __iter__(self) -> Iterator[dict]:
        yield from self.data


class CSVSink:
    """Write pipeline output to a CSV file."""

    def __init__(self, path: str | Path, **csv_kwargs):
        self.path = Path(path) if isinstance(path, str) else path
        self.csv_kwargs = csv_kwargs
        self._writer: Optional[csv.DictWriter] = None
        self._file: Optional[Any] = None

    def write(self, rows: Iterator[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "w", newline="", **self.csv_kwargs)
        first = next(rows, None)
        if first is None:
            self._file.close()
            return
        self._writer = csv.DictWriter(self._file, fieldnames=list(first.keys()))
        self._writer.writeheader()
        self._writer.writerow(first)
        for row in rows:
            self._writer.writerow(row)
        self._file.close()


class JSONSink:
    """Write pipeline output to a JSON or JSON Lines file."""

    def __init__(self, path: str | Path, lines: bool = False, indent: int = 2):
        self.path = Path(path) if isinstance(path, str) else path
        self.lines = lines
        self.indent = indent

    def write(self, rows: Iterator[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.lines:
            with open(self.path, "w") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")
        else:
            data = list(rows)
            with open(self.path, "w") as f:
                json.dump(data, f, indent=self.indent)


class ParquetSink:
    """Write pipeline output to a Parquet file."""

    def __init__(self, path: str | Path, **parquet_kwargs):
        self.path = Path(path) if isinstance(path, str) else path
        self.parquet_kwargs = parquet_kwargs

    def write(self, rows: Iterator[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame.from_records(rows)
        df.to_parquet(self.path, **self.parquet_kwargs)


def from_url(url: str, format: str | None = None) -> Callable[[], Iterator[dict]]:
    """Load data from a remote URL (CSV, JSON, Parquet)."""

    def loader() -> Iterator[dict]:
        spec = fsspec.filesystem("http")
        with spec.open(url) as f:
            if (format or url).endswith(".parquet"):
                df = pd.read_parquet(f)
                yield from df.to_dict(orient="records")
            elif (format or url).endswith(".json") or (format or url).endswith(".jsonl"):
                lines = url.endswith(".jsonl") or format == "jsonl"
                source = JSONSource(f, lines=lines)
                yield from source
            else:
                # default to CSV
                import io
                text = f.read()
                reader = csv.DictReader(io.StringIO(text))
                yield from reader

    return loader