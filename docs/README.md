# Documentation

## API Reference

### Pipeline

```python
from datapilot import Pipeline
```

### Sources

```python
from datapilot.connectors import CSVSource, JSONSource, ParquetSource
```

### Transforms

```python
from datapilot import transforms as T
T.filter(predicate)
T.map_fn(fn)
T.project(*fields)
T.deduplicate(key=...)
T.group_by(*keys)
T.aggregate(specs)
T.add_field(name, value)
T.rename_field(old, new)
T.sort_by(*keys, reverse=False)
```
