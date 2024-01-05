import typing

from beancount.core import data
from beancount_data.data_types import EntryType

from .data_types import EntryTypeConfig
from .tables import BALANCE_TABLE
from .tables import CLOSE_TABLE
from .tables import COMMODITY_TABLE
from .tables import OPEN_TABLE
from .tables import PAD_TABLE

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
}
