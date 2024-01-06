from beancount_data.data_types import Booking
from beancount_data.data_types import EntryType
from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import UUID
from sqlalchemy.dialects.postgresql import ARRAY

from .entry import Entry


class Open(Entry):
    id = Column(
        UUID(as_uuid=True),
        ForeignKey("entry.id"),
        primary_key=True,
    )
    account = Column(String, nullable=False)
    currencies = Column(ARRAY(String), nullable=True)
    booking = Column(Enum(Booking), nullable=True)

    __table_args__ = {"prefixes": ["TEMPORARY"]}
    __mapper_args__ = {"polymorphic_identity": EntryType.OPEN}
