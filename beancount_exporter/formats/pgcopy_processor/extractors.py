import uuid

from beancount.core import data


def extract_open(
    id: uuid.UUID,
    entry: data.Open,
) -> tuple:
    return (
        id,
        entry.account,
        entry.currencies,
        entry.booking.value if entry.booking is not None else None,
    )
