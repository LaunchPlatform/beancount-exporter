import decimal
import enum
import json
import sys
import typing

from beancount.core import data
from beancount.loader import LoadError
from beancount.parser.grammar import ValueType
from beancount_data.data_types import Amount
from beancount_data.data_types import Balance
from beancount_data.data_types import Close
from beancount_data.data_types import Commodity
from beancount_data.data_types import Custom
from beancount_data.data_types import Document
from beancount_data.data_types import Event
from beancount_data.data_types import Note
from beancount_data.data_types import Open
from beancount_data.data_types import Pad
from beancount_data.data_types import Price
from beancount_data.data_types import Transaction
from beancount_data.data_types import ValidationError
from beancount_data.data_types import ValidationResult
from pydantic import BaseModel

from .processor import Processor

ENTRY_TYPE_MODEL_MAP: dict[typing.Type, BaseModel] = {
    data.Open: Open,
    data.Close: Close,
    data.Commodity: Commodity,
    data.Pad: Pad,
    data.Balance: Balance,
    data.Transaction: Transaction,
    data.Note: Note,
    data.Event: Event,
    data.Price: Price,
    data.Document: Document,
    data.Custom: Custom,
}


class OptionEncoder(json.JSONEncoder):
    def default(self, obj: typing.Any) -> typing.Any:
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        elif isinstance(obj, enum.Enum):
            return obj.value
        return json.JSONEncoder.default(self, obj)


def convert_custom_value(
    value_type: ValueType,
) -> typing.Union[Amount, decimal.Decimal, str, bool]:
    if value_type.dtype in {str, bool, decimal.Decimal}:
        return value_type.value
    elif value_type.dtype is data.Amount:
        return Amount.from_orm(value_type.value)
    else:
        raise ValueError(f"Unexpected value type {value_type.dtype}")


class JsonProcessor(Processor):
    def start(self):
        pass

    def stop(self):
        sys.stdout.flush()

    def process_options(self, options: dict[str, typing.Any]):
        print(json.dumps(options, cls=OptionEncoder))
        print()

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

        print(validation_result.json())
        print()

    def process_entries(self, entries: data.Entries):
        for entry in entries:
            model_cls = ENTRY_TYPE_MODEL_MAP[type(entry)]
            if isinstance(entry, data.Custom):
                model = Custom(
                    date=entry.date,
                    meta=entry.meta,
                    type=entry.type,
                    values=list(map(convert_custom_value, entry.values)),
                )
            else:
                model = model_cls.from_orm(entry)
            filename = model.meta.get("filename")
            if filename is not None:
                model.meta["filename"] = self.strip_path(filename)
            if isinstance(model, Transaction):
                for posting in model.postings:
                    if posting.meta is None:
                        # For posting generated from pad or other plugins, they may
                        # not have meta value at all
                        continue
                    posting_filename = posting.meta.get("filename")
                    if posting_filename is None:
                        continue
                    posting.meta["filename"] = self.strip_path(posting_filename)
            elif isinstance(model, Document):
                model.filename = self.strip_path(model.filename)
            print(model.json())
