import uuid

import orjson
from beancount.core import data
from beancount_data.data_types import EntryType

from .utils import orjson_default


def extract_entry(
    id: uuid.UUID,
    entry_type: EntryType,
    entry: data.Union,
) -> tuple:
    return (
        id,
        entry_type.value,
        entry.date,
        orjson.dumps(entry.meta, default=orjson_default),
    )


def extract_open(
    id: uuid.UUID,
    entry: data.Open,
) -> tuple:
    return (
        id,
        entry.account,
        # TODO:
    )
