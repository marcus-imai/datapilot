"""Core pipeline execution engine."""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterable, Iterator, Optional

logger = logging.getLogger(__name__)


class Pipeline:
    """A composable data processing pipeline.

    Usage::

        pipeline = Pipeline(source=csv_source) | filter_fn | transform_fn
        for row in pipeline.run():
            print(row)
    """

    def __init__(
        self,
        source: Optional[Iterable[dict]] = None,
        *,
        name: str = "anonymous",
    ):
        self._source = source
        self._steps: list[Callable[[Iterator[dict]], Iterator[dict]]] = []
        self._name = name

    def __or__(self, step: Callable[[Iterator[dict]], Iterator[dict]]) -> "Pipeline":
        """Allows using the | operator to chain steps."""
        new = Pipeline(name=self._name)
        new._source = self._source
        new._steps = self._steps + [step]
        return new

    def add_step(self, fn: Callable[[Iterator[dict]], Iterator[dict]]) -> "Pipeline":
        """Programmatically add a processing step."""
        return self.__or__(fn)

    def run(self) -> Iterator[dict]:
        """Execute the pipeline and yield processed records."""
        logger.info("Starting pipeline: %s", self._name)

        iterator: Iterator[dict] = iter(self._source or [])
        for i, step in enumerate(self._steps):
            logger.debug("Step %d: %s", i + 1, step.__name__)
            iterator = step(iterator)

        yield from iterator
        logger.info("Pipeline '%s' completed.", self._name)

    def to_list(self) -> list[dict]:
        """Execute and collect all results into a list."""
        return list(self.run())

    def count(self) -> int:
        """Execute and return the number of processed records."""
        return sum(1 for _ in self.run())