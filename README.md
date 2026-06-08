# Datapilot

> Lightweight data processing pipeline framework for ETL, stream processing, and batch jobs.

[![PyPI Version](https://img.shields.io/pypi/v/datapilot.svg)](https://pypi.org/project/datapilot/)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Features

- **Pipeline DSL** — Compose data flows with a clean, chainable API
- **Built-in Transforms** — Filter, map, aggregate, join, deduplicate out of the box
- **Connector Abstraction** — Pluggable sources and sinks (CSV, JSON, Parquet, PostgreSQL, S3)
- **Streaming & Batch** — Same API works for both micro-batch and true streaming
- **CLI Tools** — Inspect, run, and monitor pipelines from the terminal
- **Extensible** — Register custom transforms, connectors, and validators

## Quick Start

```bash
pip install datapilot
```

```python
from datapilot import Pipeline, sources, sinks, transforms as T

pipeline = (
    Pipeline(source=CSVSource("sales.csv"))
    | T.filter(row => row["amount"] > 0)
    | T.group_by("region")
    | T.aggregate({"total": ("amount", "sum"), "count": ("id", "count")})
    | T.to_parquet("output.parquet")
)

pipeline.run()
```

## Installation

```bash
pip install datapilot

# with extras
pip install datapilot[postgres]   # PostgreSQL connector
pip install datapilot[aws]        # S3 / Athena connector
pip install datapilot[all]        # All connectors
```

## Documentation

Full docs at [https://marcus-imai.github.io/datapilot](https://marcus-imai.github.io/datapilot)

## License

MIT © 2025 Marcus Imai

