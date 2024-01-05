import os
import pathlib
import textwrap
import uuid

import pytest
from beancount_data.data_types import Booking
from beancount_data.data_types import EntryType
from click.testing import CliRunner
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Date
from sqlalchemy import Engine
from sqlalchemy import Enum
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import Session

from beancount_exporter.main import main


@as_declarative()
class Base:
    id: uuid.UUID
    __name__: str

    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


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


@pytest.fixture
def engine() -> Engine:
    return create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)


@pytest.fixture
def db(engine: Engine) -> Session:
    session = Session(bind=engine)
    Base.metadata.create_all(bind=engine)
    try:
        yield session
    finally:
        session.close()


def test_output(tmp_path: pathlib.Path, db: Session):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    bean_file_path = tmp_path / "main.bean"
    bean_file_path.write_text(
        textwrap.dedent(
            """\
    1970-01-01 open Assets:Checking
    1970-01-01 open Equity:Opening-Balances
    2023-02-10 pad Assets:Checking Equity:Opening-Balances
    2023-03-22 balance Assets:Checking 123.45 USD    """
        )
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            str(bean_file_path),
            "--base-path",
            str(tmp_path),
            "--format",
            "PGCOPY",
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    assert not result.exception
    assert not result.output

    conn = db.connection()
    cursor = conn.connection.cursor()
    with open(output_dir / "entry_base.pgcopy.bin", "rb") as fo:
        cursor.copy_expert(
            "COPY entry FROM STDIN WITH (FORMAT BINARY)",
            file=fo,
        )

    assert db.query(Entry).all()
