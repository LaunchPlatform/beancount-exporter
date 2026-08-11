import pathlib
import typing

from beancount.core import data
from beancount.loader import LoadError

from ..utils import strip_base_path


class Processor:
    def __init__(
        self,
        base_path: pathlib.Path,
        strip_paths: bool = True,
        path_cache: dict[str, str] | None = None,
    ):
        self.base_path = base_path
        self.strip_paths = strip_paths
        self.path_cache = {} if path_cache is None else path_cache

    def strip_path(self, path: str | list[str]) -> str | list[str]:
        if not self.strip_paths:
            return path
        return strip_base_path(
            path, base_path=self.base_path, cache=self.path_cache
        )

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def process_options(self, options: dict[str, typing.Any]):
        raise NotImplementedError()

    def process_errors(self, errors: list[LoadError]):
        raise NotImplementedError()

    def process_entries(self, entries: data.Entries):
        raise NotImplementedError()
