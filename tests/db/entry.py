from beancount_data.data_types import EntryType
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import Enum
from sqlalchemy import UUID
from sqlalchemy.dialects.postgresql import JSONB

from .base import Base


class Entry(Base):
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
    )
    entry_type = Column(Enum(EntryType), nullable=False)
    date = Column(Date, nullable=False)
    meta = Column(JSONB, nullable=False)
    __table_args__ = {"prefixes": ["TEMPORARY"]}
    __mapper_args__ = {
        "polymorphic_on": entry_type,
    }
