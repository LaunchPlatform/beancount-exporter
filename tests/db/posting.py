from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DECIMAL
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .entry import Base


class Posting(Base):
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
    )
    transaction_id = Column(
        UUID(as_uuid=True),
        ForeignKey("transaction.id"),
        nullable=False,
    )
    account = Column(String, nullable=False)
    units_number = Column(DECIMAL, nullable=True)
    units_currency = Column(String, nullable=False)
    price_number = Column(DECIMAL, nullable=True)
    price_currency = Column(String, nullable=True)

    cost_number = Column(DECIMAL, nullable=True)
    cost_currency = Column(String, nullable=True)
    cost_date = Column(Date, nullable=True)
    cost_label = Column(String, nullable=True)
    cost_number_per = Column(DECIMAL, nullable=True)
    cost_number_total = Column(DECIMAL, nullable=True)
    cost_merge = Column(Boolean, nullable=True)

    flag = Column(String, nullable=True)
    meta = Column(JSONB, nullable=False)

    transaction = relationship(
        "Transaction",
        back_populates="postings",
        uselist=False,
    )

    __table_args__ = {"prefixes": ["TEMPORARY"]}
