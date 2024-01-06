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
from beancount_data.data_types import Transaction
from beancount_data.data_types import ValidationError
from beancount_data.data_types import ValidationResult

from ..processor import Processor
from .configs import ENTRY_TYPE_CONFIGS
from .configs import EntryTypeConfig
from .data_types import Table
from .tables import ENTRY_BASE_TABLE
from .tables import POSTING_TABLE
from .utils import compile_formatter
from .utils import convert_custom_value
from .utils import orjson_default
from .utils import orjson_option_maps_default
from .utils import serialize_row


class PgCopyProcessor(Processor):
    def __init__(
        self,
        base_path: pathlib.Path,
        option_maps_file: io.BytesIO,
        errors_file: io.BytesIO,
        entry_base_file: io.BytesIO,
        posting_file: io.BytesIO,
        entry_files: dict[typing.Type, io.BytesIO],
        entry_base_table: Table = ENTRY_BASE_TABLE,
        posting_table: Table = POSTING_TABLE,
        entry_configs: dict[typing.Type, EntryTypeConfig] | None = None,
        encoding: str = "utf8",
    ):
        super().__init__(base_path=base_path)
        self.option_maps_file = option_maps_file
        self.errors_file = errors_file
        self.entry_base_file = entry_base_file
        self.posting_file = posting_file
        self.entry_files = entry_files
        self.entry_base_table = entry_base_table
        self.posting_table = posting_table
        self.entry_configs = entry_configs or ENTRY_TYPE_CONFIGS
        self.encoding = encoding
        self._entry_base_formatters = self._compile_formatters(self.entry_base_table)
        self._posting_formatters = self._compile_formatters(self.posting_table)
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

    def _extract_close(self, id: uuid.UUID, entry: data.Close) -> tuple:
        return (
            id,
            entry.account,
        )

    def _extract_commodity(self, id: uuid.UUID, entry: data.Commodity) -> tuple:
        return (
            id,
            entry.currency,
        )

    def _extract_pad(self, id: uuid.UUID, entry: data.Pad) -> tuple:
        return (
            id,
            entry.account,
            entry.source_account,
        )

    def _extract_balance(self, id: uuid.UUID, entry: data.Balance) -> tuple:
        return (
            id,
            entry.account,
            entry.amount.number,
            entry.amount.currency,
            entry.tolerance,
            entry.diff_amount.number if entry.diff_amount is not None else None,
            entry.diff_amount.currency if entry.diff_amount is not None else None,
        )

    def _extract_transaction(self, id: uuid.UUID, entry: data.Transaction) -> tuple:
        return (
            id,
            entry.flag,
            entry.payee,
            entry.narration,
            # pgcopy doesn't recognize frozenset
            # TODO: fix that issue in the upstream
            set(entry.tags),
            set(entry.links),
        )

    def _extract_note(self, id: uuid.UUID, entry: data.Note) -> tuple:
        return (
            id,
            entry.account,
            entry.comment,
        )

    def _extract_event(self, id: uuid.UUID, entry: data.Event) -> tuple:
        return (
            id,
            entry.type,
            entry.description,
        )

    def _extract_price(self, id: uuid.UUID, entry: data.Price) -> tuple:
        return (
            id,
            entry.currency,
            entry.amount.number,
            entry.amount.currency,
        )

    def _extract_document(self, id: uuid.UUID, entry: data.Document) -> tuple:
        return (
            id,
            entry.account,
            self.strip_path(entry.filename),
            # pgcopy doesn't recognize frozenset
            # TODO: fix that issue in the upstream
            set(entry.tags),
            set(entry.links),
        )

    def _extract_custom(self, id: uuid.UUID, entry: data.Custom) -> tuple:
        return (
            id,
            entry.type,
            list(map(convert_custom_value, entry.values)),
        )

    def _extract_posting(
        self, id: uuid.UUID, transaction_id: uuid.UUID, posting: data.Posting
    ) -> tuple:
        if isinstance(posting.cost, data.CostSpec):
            cost_spec = (
                posting.cost.number_per,
                posting.cost.number_total,
                posting.cost.merge,
            )
        else:
            cost_spec = (None, None, None)

        meta = posting.meta
        if posting.meta is not None:
            meta = meta.copy()
            filename = meta.get("filename")
            if filename is not None:
                meta["filename"] = self.strip_path(filename)

        return (
            id,
            transaction_id,
            posting.account,
            posting.units.number,
            posting.units.currency,
            posting.price.number if posting.price is not None else None,
            posting.price.currency if posting.price is not None else None,
            posting.cost.number if isinstance(posting.cost, data.Cost) else None,
            posting.cost.currency if posting.cost is not None else None,
            posting.cost.date if posting.cost is not None else None,
            posting.cost.label if posting.cost is not None else None,
            *cost_spec,
            posting.flag,
            orjson.dumps(meta, default=orjson_default),
        )

    def _process_transaction(self, transaction_id: uuid.UUID, entry: data.Transaction):
        for posting in entry.postings:
            posting_values = self._extract_posting(
                uuid.uuid4(), transaction_id, posting
            )
            self.posting_file.write(
                serialize_row(self._posting_formatters, posting_values)
            )

    @property
    def all_files(self) -> tuple[io.BytesIO, ...]:
        return self.entry_base_file, self.posting_file, *self.entry_files.values()

    def start(self):
        for pgcopy_file in self.all_files:
            pgcopy_file.write(pgcopy.copy.BINCOPY_HEADER)

    def stop(self):
        for pgcopy_file in self.all_files:
            pgcopy_file.write(pgcopy.copy.BINCOPY_TRAILER)

    def process_options(self, options: dict[str, typing.Any]):
        self.option_maps_file.write(
            orjson.dumps(options, default=orjson_option_maps_default)
        )

    def process_errors(self, errors: list[LoadError]):
        validation_result = ValidationResult(
            errors=list(map(ValidationError.from_orm, errors))
        )
        for error in validation_result.errors:
            filename = error.source.get("filename")
            if filename is not None:
                error.source["filename"] = self.strip_path(filename)
            if error.entry is not None:
                if "filename" in error.entry.meta:
                    error.entry.meta["filename"] = self.strip_path(
                        error.entry.meta["filename"]
                    )
                if isinstance(error.entry, Transaction):
                    for posting in error.entry.postings:
                        posting_filename = posting.meta.get("filename")
                        if posting_filename is None:
                            continue
                        posting.meta["filename"] = self.strip_path(posting_filename)
        self.errors_file.write(validation_result.json().encode("utf8"))

    def process_entries(self, entries: data.Entries):
        extractors = {
            data.Open: self._extract_open,
            data.Close: self._extract_close,
            data.Commodity: self._extract_commodity,
            data.Pad: self._extract_pad,
            data.Balance: self._extract_balance,
            data.Transaction: self._extract_transaction,
            data.Note: self._extract_note,
            data.Event: self._extract_event,
            data.Price: self._extract_price,
            data.Document: self._extract_document,
            data.Custom: self._extract_custom,
        }
        # TODO: to improve performance even more, maybe we can have multiprocessing
        #       breaking down entries into groups first and process them in different
        #       thread / processors
        for entry in entries:
            entry_type = type(entry)
            entry_config = self.entry_configs[entry_type]
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
            if entry_type is data.Transaction:
                self._process_transaction(entry_id, entry)
