import datetime
import pathlib
import typing

import flatbuffers
from beancount.core import data
from beancount.loader import LoadError
from beancount_data.fbs.data_types import Close
from beancount_data.fbs.data_types import Date
from beancount_data.fbs.data_types import Entry
from beancount_data.fbs.data_types import MapItem
from beancount_data.fbs.data_types import MapValueType
from beancount_data.fbs.data_types import Open

from .processor import Processor


class FlatbuffersProcessor(Processor):
    def __init__(self, base_path: pathlib.Path):
        super().__init__(base_path)
        self._builder = flatbuffers.Builder()

    def process_options(self, options: dict[str, typing.Any]):
        pass

    def process_errors(self, errors: list[LoadError]):
        pass

    def process_entries(self, entries: data.Entries):
        create_func_map: dict[type, typing.Callable] = {
            data.Open: self._make_open,
        }
        for entry in entries:
            create_func = create_func_map[type(entry)]
            payload = create_func(entry)

            Entry.EntryStart(self._builder)
            Entry.EntryAddDate(self._builder, self._make_date(entry.date))
            Entry.EntryAddPayload(self._builder, payload)

    def _make_date(self, date: datetime.date) -> Date.Date:
        return Date.CreateDate(
            self._builder, year=date.year, month=date.month, day=date.day
        )

    def _make_meta(self, meta: dict) -> int:
        MapItem.StartBsonValueVector(self._builder, len(meta))
        for key, value in meta.items():
            MapItem.MapItemAddKey(self._builder, self._builder.CreateString(key))
        return self._builder.EndVector()

    def _make_meta_value(self, value: typing.Any) -> int:
        pass

    def _make_open(self, entry: data.Open) -> Open.Open:
        Open.OpenStart(self._builder)
        Open.OpenAddAccount(self._builder, self._builder.CreateString(entry.account))
        if entry.currencies:
            Open.OpenAddCurrencies(self._builder, entry.currencies)
        if entry.booking is not None:
            Open.AddBooking(self._builder, entry.booking)
        return Open.End(self._builder)
