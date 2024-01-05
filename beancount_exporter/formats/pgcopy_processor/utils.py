import decimal
import functools
import struct
import typing

import pgcopy

from .data_types import Column


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
