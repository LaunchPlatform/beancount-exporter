from beancount_data.data_types import EntryType
from sqlalchemy import Column
from sqlalchemy import DECIMAL
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import UUID

from .entry import Entry


class Balance(Entry):
    id = Column(
        UUID(as_uuid=True),
        ForeignKey("entry.id"),
        primary_key=True,
    )
    account = Column(String, nullable=False)
    amount_number = Column(DECIMAL, nullable=True)
    amount_currency = Column(String, nullable=False)
    tolerance = Column(DECIMAL, nullable=True)
    diff_number = Column(DECIMAL, nullable=True)
    diff_currency = Column(String, nullable=True)

    __table_args__ = {"prefixes": ["TEMPORARY"]}
    __mapper_args__ = {"polymorphic_identity": EntryType.BALANCE}
