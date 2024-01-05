import typing

from beancount.core import data
from beancount_data.data_types import EntryType

from .data_types import EntryTypeConfig
from .extractors import extract_open
from .tables import OPEN_TABLE

ENTRY_TYPE_CONFIGS: dict[typing.Type, EntryTypeConfig] = {
    data.Open: EntryTypeConfig(
        type=EntryType.OPEN,
        table=OPEN_TABLE,
        extractor=extract_open,
    ),
}
