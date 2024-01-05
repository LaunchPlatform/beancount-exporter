import datetime
import os
import pathlib
import textwrap
import typing

import pytest
from beancount_data.data_types import Booking
from beancount_data.data_types import EntryType
from click.testing import CliRunner
from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from .db.base import Base
from .db.close import Close
from .db.commodity import Commodity
from .db.entry import Entry
from .db.open import Open
from beancount_exporter.main import main

MakeBeanfileFunc = typing.Callable[[str], pathlib.Path]
ExportEntriesFunc = typing.Callable[[pathlib.Path], pathlib.Path]
ImportTableFunc = typing.Callable[[pathlib.Path, str], None]


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


@pytest.fixture
def make_beanfile(tmp_path: pathlib.Path) -> MakeBeanfileFunc:
    def _make_bean_file(content: str) -> pathlib:
        bean_file_path = tmp_path / "main.bean"
        bean_file_path.write_text(textwrap.dedent(content))
        return bean_file_path

    return _make_bean_file


@pytest.fixture
def export_entries(tmp_path: pathlib.Path) -> ExportEntriesFunc:
    def _export_entries(beanfile: pathlib.Path) -> pathlib.Path:
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                str(beanfile),
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
        return output_dir

    return _export_entries


@pytest.fixture
def import_table(db: Session) -> ImportTableFunc:
    def _import_table(pgcopy_path: pathlib.Path, statement: str):
        conn = db.connection()
        cursor = conn.connection.cursor()
        with open(pgcopy_path, "rb") as fo:
            cursor.copy_expert(
                statement,
                file=fo,
            )

    return _import_table


def test_opens(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 open Assets:Checking USD,TWD
    1970-01-02 open Assets:WindFalls BTC
    1970-01-03 open Assets:Stocks:Tesla TSLA "FIFO"
    1970-01-04 open Equity:Opening-Balances
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(output_dir / "open.bin", "COPY open FROM STDIN WITH (FORMAT BINARY)")
    opens = db.query(Open).order_by(Open.date).all()
    assert len(opens) == 4
    assert db.query(Entry).count() == len(opens)

    open0 = opens[0]
    assert open0.date == datetime.date(1970, 1, 1)
    assert open0.entry_type == EntryType.OPEN
    assert open0.account == "Assets:Checking"
    assert frozenset(open0.currencies) == frozenset(["USD", "TWD"])
    assert open0.booking is None

    open1 = opens[1]
    assert open1.date == datetime.date(1970, 1, 2)
    assert open1.entry_type == EntryType.OPEN
    assert open1.account == "Assets:WindFalls"
    assert frozenset(open1.currencies) == frozenset(["BTC"])
    assert open1.booking is None

    open2 = opens[2]
    assert open2.date == datetime.date(1970, 1, 3)
    assert open2.entry_type == EntryType.OPEN
    assert open2.account == "Assets:Stocks:Tesla"
    assert frozenset(open2.currencies) == frozenset(["TSLA"])
    assert open2.booking == Booking.FIFO

    open3 = opens[3]
    assert open3.date == datetime.date(1970, 1, 4)
    assert open3.entry_type == EntryType.OPEN
    assert open3.account == "Equity:Opening-Balances"
    assert open3.currencies is None
    assert open3.booking is None


def test_close(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 open Assets:Checking
    1970-01-02 open Assets:WindFalls
    1970-01-03 open Equity:Opening-Balances
    1970-01-04 close Assets:Checking
    1970-01-05 close Equity:Opening-Balances
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(output_dir / "close.bin", "COPY close FROM STDIN WITH (FORMAT BINARY)")
    closes = db.query(Close).order_by(Close.date).all()
    assert len(closes) == 2

    close0 = closes[0]
    assert close0.date == datetime.date(1970, 1, 4)
    assert close0.entry_type == EntryType.CLOSE
    assert close0.account == "Assets:Checking"

    close1 = closes[1]
    assert close1.date == datetime.date(1970, 1, 5)
    assert close1.entry_type == EntryType.CLOSE
    assert close1.account == "Equity:Opening-Balances"


def test_commodity(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 commodity BTC
    1970-01-02 commodity ETH
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(
        output_dir / "commodity.bin", "COPY commodity FROM STDIN WITH (FORMAT BINARY)"
    )
    commodities = db.query(Commodity).order_by(Commodity.date).all()
    assert len(commodities) == 2
    assert db.query(Entry).count() == len(commodities)

    commodity0 = commodities[0]
    assert commodity0.date == datetime.date(1970, 1, 1)
    assert commodity0.entry_type == EntryType.COMMODITY
    assert commodity0.currency == "BTC"

    commodity1 = commodities[1]
    assert commodity1.date == datetime.date(1970, 1, 2)
    assert commodity1.entry_type == EntryType.COMMODITY
    assert commodity1.currency == "ETH"
