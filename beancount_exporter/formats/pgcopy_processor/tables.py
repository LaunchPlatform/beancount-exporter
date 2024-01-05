from .data_types import Column
from .data_types import Table

ID_COLUMN = Column(
    attname="id",
    type_category="U",
    type_name="uuid",
    type_mod=-1,
    not_null=True,
    typelem=0,
)
ENTRY_BASE_TABLE: Table = (
    ID_COLUMN,
    Column(
        attname="entry_type",
        type_category="E",
        type_name="entrytype",
        type_mod=-1,
        not_null=True,
        typelem=0,
    ),
    Column(
        attname="date",
        type_category="D",
        type_name="date",
        type_mod=-1,
        not_null=True,
        typelem=0,
    ),
    Column(
        attname="meta",
        type_category="U",
        type_name="jsonb",
        type_mod=-1,
        not_null=True,
        typelem=0,
    ),
)
OPEN_TABLE: Table = (
    ID_COLUMN,
    Column(
        attname="account",
        type_category="S",
        type_name="varchar",
        type_mod=-1,
        not_null=True,
        typelem=0,
    ),
    Column(
        attname="currencies",
        type_category="A",
        type_name="varchar",
        type_mod=-1,
        not_null=False,
        # varchar's type oid is 1043
        # ref: https://github.com/postgres/postgres/blob/14dd0f27d7cd56ffae9ecdbe324965073d01a9ff/src/include/catalog/pg_type.dat#L280-L286
        typelem=1043,
    ),
    Column(
        attname="booking",
        type_category="E",
        type_name="booking",
        type_mod=-1,
        not_null=False,
        typelem=0,
    ),
)
CLOSE_TABLE: Table = (
    ID_COLUMN,
    Column(
        attname="account",
        type_category="S",
        type_name="varchar",
        type_mod=-1,
        not_null=True,
        typelem=0,
    ),
)
