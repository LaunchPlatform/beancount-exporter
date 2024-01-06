from beancount_data.data_types import EntryType
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .entry import Entry


class Transaction(Entry):
    id = Column(
        UUID(as_uuid=True),
        ForeignKey("entry.id"),
        primary_key=True,
    )
    flag = Column(String, nullable=False)
    payee = Column(String, nullable=True)
    narration = Column(String, nullable=False)
    tags = Column(ARRAY(String), nullable=False)
    links = Column(ARRAY(String), nullable=False)

    postings = relationship(
        "Posting",
        back_populates="transaction",
        cascade="all,delete",
    )
    __table_args__ = {"prefixes": ["TEMPORARY"]}
    __mapper_args__ = {"polymorphic_identity": EntryType.TRANSACTION}
