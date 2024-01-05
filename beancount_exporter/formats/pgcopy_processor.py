import functools
import struct
import typing

import orjson
import pgcopy
from beancount.core import data
from beancount.loader import LoadError

from .processor import Processor

ENCODING = "utf8"


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


DATE_COLUMN = Column(
    attname="date",
    type_category="D",
    type_name="date",
    type_mod=-1,
    not_null=True,
    typelem=0,
)
META_COLUMN = Column(
    attname="meta",
    type_category="U",
    type_name="jsonb",
    type_mod=-1,
    not_null=True,
    typelem=0,
)
SHARED_COLUMNS = (
    DATE_COLUMN,
    META_COLUMN,
)

OPEN_TABLE = (
    *SHARED_COLUMNS,
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


def compile_formatter(column: Column, encoding: str = ENCODING):
    funcs = [
        pgcopy.copy.encode,
        pgcopy.copy.maxsize,
        pgcopy.copy.array,
        pgcopy.copy.diagnostic,
        pgcopy.copy.null,
    ]
    reducer_func = lambda f, mf: mf(column, encoding, f)
    return functools.reduce(reducer_func, funcs, pgcopy.copy.get_formatter(column))


class PgCopyProcessor(Processor):
    def process_options(self, options: dict[str, typing.Any]):
        pass

    def process_errors(self, errors: list[LoadError]):
        pass

    def process_entries(self, entries: data.Entries):
        formatters = list(map(compile_formatter, OPEN_TABLE))
        with open("open.pgcopy.bin", "wb") as fo:
            fo.write(pgcopy.copy.BINCOPY_HEADER)
            for entry in entries:
                row_fmt = [">h"]
                row_rdat = [len(OPEN_TABLE)]
                if isinstance(entry, data.Open):
                    for formatter, value in zip(
                        formatters,
                        [
                            entry.date,
                            orjson.dumps(entry.meta),
                            entry.account,
                            entry.currencies,
                            entry.booking if entry.booking is not None else None,
                        ],
                    ):
                        fmt, values = formatter(value)
                        row_fmt.append(fmt)
                        row_rdat.extend(values)
                    fo.write(struct.pack("".join(row_fmt), *row_rdat))
            fo.write(pgcopy.copy.BINCOPY_TRAILER)
