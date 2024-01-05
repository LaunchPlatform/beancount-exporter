from beancount_data.data_types import EntryType
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import UUID

from .entry import Entry


class Commodity(Entry):
    id = Column(
        UUID(as_uuid=True),
        ForeignKey("entry.id"),
        primary_key=True,
    )
    currency = Column(String, nullable=False)

    __table_args__ = {"prefixes": ["TEMPORARY"]}
    __mapper_args__ = {"polymorphic_identity": EntryType.COMMODITY}
