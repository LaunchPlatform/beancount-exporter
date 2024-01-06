import datetime
import decimal
import functools
import json
import struct
import typing

import pgcopy.copy
from beancount.core import account
from beancount.core import data
from beancount.parser.grammar import ValueType

from .data_types import Column


def convert_custom_value(
    value_type: ValueType,
) -> str:
    if value_type.dtype in {str, bool, decimal.Decimal}:
        return json.dumps(value_type.value)
    elif value_type.dtype in {decimal.Decimal}:
        return json.dumps(str(value_type.value))
    elif value_type.dtype in {datetime.date, account.TYPE}:
        return str(value_type.value)
    elif value_type.dtype is data.Amount:
        return json.dumps(
            dict(
                number=str(value_type.value.number), currency=value_type.value.currency
            )
        )
    else:
        raise ValueError(f"Unexpected value type {value_type.dtype}")


def orjson_default(value: typing.Any) -> typing.Any:
    if isinstance(value, decimal.Decimal):
        return str(value)
    raise TypeError


def compile_formatter(encoding: str, column: Column):
    funcs = [
        pgcopy.copy.encode,
        pgcopy.copy.maxsize,
        pgcopy.copy.array,
        pgcopy.copy.diagnostic,
        pgcopy.copy.null,
    ]

    def reducer_func(f, mf):
        return mf(column, encoding, f)

    return functools.reduce(reducer_func, funcs, pgcopy.copy.get_formatter(column))


def serialize_row(formatters: list[typing.Callable], values: tuple) -> bytes:
    row_fmt = [">h"]
    row_values = [len(formatters)]
    for formatter, value in zip(
        formatters,
        values,
    ):
        fmt, values = formatter(value)
        row_fmt.append(fmt)
        row_values.extend(values)
    return struct.pack("".join(row_fmt), *row_values)
