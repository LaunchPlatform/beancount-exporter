import typing
import uuid

from beancount.core import data
from beancount_data.data_types import EntryType


class Column(typing.NamedTuple):
    # attribute name of the column, like `account`
    attname: str
    # PostgreSQL type category, e.g, U standards for User-defined types, S for string
    # and E for enum
    # ref: https://www.postgresql.org/docs/current/catalog-pg-type.html
    type_category: str
    # Name of the type, such as `uuid`, `varchar` or `booking` (custom enum type)
    type_name: str
    # Type length for string (could be other use?), -1 means unlimited
    type_mod: int
    # Is this attribute NOT NULL
    not_null: bool
    # Element type OID of array (0 means this is not array)
    typelem: int


Table = tuple[Column, ...]
Extractor = typing.Callable[[uuid.UUID, data.Union], tuple]


class EntryTypeConfig(typing.NamedTuple):
    type: EntryType
    table: Table
    extractor: Extractor
