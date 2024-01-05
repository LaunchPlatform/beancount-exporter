import functools
import io
import pathlib
import typing
import uuid

import orjson
import pgcopy
from beancount.core import data
from beancount.loader import LoadError
from beancount_data.data_types import EntryType

from ..processor import Processor
from .configs import ENTRY_TYPE_CONFIGS
from .configs import EntryTypeConfig
from .data_types import Table
from .tables import ENTRY_BASE_TABLE
from .utils import compile_formatter
from .utils import orjson_default
from .utils import serialize_row


class PgCopyProcessor(Processor):
    def __init__(
        self,
        base_path: pathlib.Path,
        entry_base_file: io.BytesIO,
        entry_files: dict[typing.Type, io.BytesIO],
        entry_base_table: Table = ENTRY_BASE_TABLE,
        entry_configs: dict[typing.Type, EntryTypeConfig] | None = None,
        encoding: str = "utf8",
    ):
        super().__init__(base_path=base_path)
        self.entry_base_file = entry_base_file
        self.entry_files = entry_files
        self.entry_base_table = entry_base_table
        self.entry_configs = entry_configs or ENTRY_TYPE_CONFIGS
        self.encoding = encoding
        self._entry_base_formatters = self._compile_formatters(ENTRY_BASE_TABLE)
        self._formatters = {
            key: self._compile_formatters(config.table)
            for key, config in self.entry_configs.items()
        }

    def _compile_formatters(self, table: Table) -> list[typing.Callable]:
        return list(map(functools.partial(compile_formatter, self.encoding), table))

    def _extract_entry(
        self,
        id: uuid.UUID,
        entry_type: EntryType,
        entry: data.Union,
    ) -> tuple:
        meta = entry.meta
        filename = meta.get("filename")
        if filename is not None:
            meta["filename"] = self.strip_path(filename)
        return (
            id,
            entry_type.name,
            entry.date,
            orjson.dumps(meta, default=orjson_default),
        )

    def _extract_open(self, id: uuid.UUID, entry: data.Open) -> tuple:
        return (
            id,
            entry.account,
            entry.currencies,
            entry.booking.value if entry.booking is not None else None,
        )

    @property
    def all_files(self) -> tuple[io.BytesIO, ...]:
        return self.entry_base_file, *self.entry_files.values()

    def start(self):
        for pgcopy_file in self.all_files:
            pgcopy_file.write(pgcopy.copy.BINCOPY_HEADER)

    def stop(self):
        for pgcopy_file in self.all_files:
            pgcopy_file.write(pgcopy.copy.BINCOPY_TRAILER)

    def process_options(self, options: dict[str, typing.Any]):
        pass

    def process_errors(self, errors: list[LoadError]):
        pass

    def process_entries(self, entries: data.Entries):
        extractors = {data.Open: self._extract_open}
        # TODO: to improve performance even more, maybe we can have multiprocessing
        #       breaking down entries into groups first and process them in different
        #       thread / processors
        for entry in entries:
            entry_type = type(entry)
            entry_config = self.entry_configs.get(entry_type)
            if entry_config is None:
                # XXX:
                continue
            entry_id = uuid.uuid4()
            entry_base_values = self._extract_entry(
                entry_id,
                entry_config.type,
                entry,
            )
            self.entry_base_file.write(
                serialize_row(self._entry_base_formatters, entry_base_values)
            )

            extractor = extractors[entry_type]
            entry_values = extractor(entry_id, entry)
            entry_file = self.entry_files[entry_type]
            entry_formatters = self._formatters[entry_type]
            entry_file.write(serialize_row(entry_formatters, entry_values))
