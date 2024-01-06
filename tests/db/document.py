from beancount_data.data_types import EntryType
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID

from .entry import Entry


class Document(Entry):
    id = Column(
        UUID(as_uuid=True),
        ForeignKey("entry.id"),
        primary_key=True,
    )
    account = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    tags = Column(ARRAY(String), nullable=False)
    links = Column(ARRAY(String), nullable=False)

    __table_args__ = {"prefixes": ["TEMPORARY"]}
    __mapper_args__ = {"polymorphic_identity": EntryType.DOCUMENT}