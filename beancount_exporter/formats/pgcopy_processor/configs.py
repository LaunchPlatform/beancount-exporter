import typing

from beancount.core import data
from beancount_data.data_types import EntryType

from .data_types import EntryTypeConfig
from .tables import BALANCE_TABLE
from .tables import CLOSE_TABLE
from .tables import COMMODITY_TABLE
from .tables import CUSTOM_TABLE
from .tables import DOCUMENT_TABLE
from .tables import EVENT_TABLE
from .tables import NOTE_TABLE
from .tables import OPEN_TABLE
from .tables import PAD_TABLE
from .tables import PRICE_TABLE
from .tables import TRANSACTION_TABLE

ENTRY_TYPE_CONFIGS: dict[typing.Type, EntryTypeConfig] = {
    data.Open: EntryTypeConfig(
        type=EntryType.OPEN,
        table=OPEN_TABLE,
    ),
    data.Close: EntryTypeConfig(
        type=EntryType.CLOSE,
        table=CLOSE_TABLE,
    ),
    data.Commodity: EntryTypeConfig(
        type=EntryType.COMMODITY,
        table=COMMODITY_TABLE,
    ),
    data.Pad: EntryTypeConfig(
        type=EntryType.PAD,
        table=PAD_TABLE,
    ),
    data.Balance: EntryTypeConfig(
        type=EntryType.BALANCE,
        table=BALANCE_TABLE,
    ),
    data.Transaction: EntryTypeConfig(
        type=EntryType.TRANSACTION,
        table=TRANSACTION_TABLE,
    ),
    data.Note: EntryTypeConfig(
        type=EntryType.NOTE,
        table=NOTE_TABLE,
    ),
    data.Event: EntryTypeConfig(
        type=EntryType.EVENT,
        table=EVENT_TABLE,
    ),
    data.Price: EntryTypeConfig(
        type=EntryType.PRICE,
        table=PRICE_TABLE,
    ),
    data.Document: EntryTypeConfig(
        type=EntryType.DOCUMENT,
        table=DOCUMENT_TABLE,
    ),
    data.Custom: EntryTypeConfig(
        type=EntryType.CUSTOM,
        table=CUSTOM_TABLE,
    ),
}
