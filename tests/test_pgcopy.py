import datetime
import decimal
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

from .db.balance import Balance
from .db.base import Base
from .db.close import Close
from .db.commodity import Commodity
from .db.entry import Entry
from .db.event import Event
from .db.note import Note
from .db.open import Open
from .db.pad import Pad
from .db.price import Price
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


def test_pad(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 open Assets:Checking
    1970-01-01 open Equity:Opening-Balances
    2023-02-10 pad Assets:Checking Equity:Opening-Balances
    2023-03-22 balance Assets:Checking 123.45 USD
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(output_dir / "pad.bin", "COPY pad FROM STDIN WITH (FORMAT BINARY)")
    pads = db.query(Pad).order_by(Pad.date).all()
    assert len(pads) == 1

    pad0 = pads[0]
    assert pad0.date == datetime.date(2023, 2, 10)
    assert pad0.entry_type == EntryType.PAD
    assert pad0.account == "Assets:Checking"
    assert pad0.source_account == "Equity:Opening-Balances"


def test_balance(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 open Assets:Checking
    1970-01-01 open Income:Job
    1970-01-01 open Equity:Opening-Balances
    2023-02-10 pad Assets:Checking Equity:Opening-Balances
    2023-03-22 balance Assets:Checking 123.45 USD
    2023-03-23 * "Salary"
        Assets:Checking 500.0 USD
        Income:Job
    2023-03-24 balance Assets:Checking 623.44 ~ 0.05 USD
    
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(
        output_dir / "balance.bin", "COPY balance FROM STDIN WITH (FORMAT BINARY)"
    )
    balances = db.query(Balance).order_by(Balance.date).all()
    assert len(balances) == 2

    balance0 = balances[0]
    assert balance0.date == datetime.date(2023, 3, 22)
    assert balance0.entry_type == EntryType.BALANCE
    assert balance0.account == "Assets:Checking"
    assert balance0.amount_number == decimal.Decimal("123.45")
    assert balance0.amount_currency == "USD"
    assert balance0.tolerance is None
    assert balance0.diff_number is None
    assert balance0.diff_currency is None

    balance1 = balances[1]
    assert balance1.date == datetime.date(2023, 3, 24)
    assert balance1.entry_type == EntryType.BALANCE
    assert balance1.account == "Assets:Checking"
    assert balance1.amount_number == decimal.Decimal("623.44")
    assert balance1.amount_currency == "USD"
    assert balance1.tolerance == decimal.Decimal("0.05")
    # TODO: not sure to make this number present
    assert balance1.diff_number is None
    assert balance1.diff_currency is None


# TODO: txn


def test_note(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 open Assets:Checking
    1970-01-01 open Income:Job
    1970-01-01 open Equity:Opening-Balances
    1970-01-02 note Assets:Checking "MOCK NOTE 0"
    1970-01-03 note Income:Job "MOCK NOTE 1"
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(output_dir / "note.bin", "COPY note FROM STDIN WITH (FORMAT BINARY)")
    notes = db.query(Note).order_by(Note.date).all()
    assert len(notes) == 2

    note0 = notes[0]
    assert note0.date == datetime.date(1970, 1, 2)
    assert note0.entry_type == EntryType.NOTE
    assert note0.account == "Assets:Checking"
    assert note0.comment == "MOCK NOTE 0"

    note1 = notes[1]
    assert note1.date == datetime.date(1970, 1, 3)
    assert note1.entry_type == EntryType.NOTE
    assert note1.account == "Income:Job"
    assert note1.comment == "MOCK NOTE 1"


def test_event(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 event "location" "Paris, France"
    1970-01-02 event "country" "US"
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(output_dir / "event.bin", "COPY event FROM STDIN WITH (FORMAT BINARY)")
    events = db.query(Event).order_by(Event.date).all()
    assert len(events) == 2

    event0 = events[0]
    assert event0.date == datetime.date(1970, 1, 1)
    assert event0.entry_type == EntryType.EVENT
    assert event0.type == "location"
    assert event0.description == "Paris, France"

    event1 = events[1]
    assert event1.date == datetime.date(1970, 1, 2)
    assert event1.entry_type == EntryType.EVENT
    assert event1.type == "country"
    assert event1.description == "US"


def test_price(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 price BTC 123.45 USD
    1970-01-02 price ETH 200.12 USD
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(output_dir / "price.bin", "COPY price FROM STDIN WITH (FORMAT BINARY)")
    prices = db.query(Price).order_by(Price.date).all()
    assert len(prices) == 2

    price0 = prices[0]
    assert price0.date == datetime.date(1970, 1, 1)
    assert price0.entry_type == EntryType.PRICE
    assert price0.currency == "BTC"
    assert price0.amount_number == decimal.Decimal("123.45")
    assert price0.amount_currency == "USD"

    price1 = prices[1]
    assert price1.date == datetime.date(1970, 1, 2)
    assert price1.entry_type == EntryType.PRICE
    assert price1.currency == "ETH"
    assert price1.amount_number == decimal.Decimal("200.12")
    assert price1.amount_currency == "USD"
