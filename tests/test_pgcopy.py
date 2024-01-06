import datetime
import decimal
import os
import pathlib
import textwrap
import typing

import orjson
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
from .db.custom import Custom
from .db.document import Document
from .db.entry import Entry
from .db.event import Event
from .db.note import Note
from .db.open import Open
from .db.pad import Pad
from .db.posting import Posting
from .db.price import Price
from .db.transaction import Transaction
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
            catch_exceptions=False,
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


def test_option_maps(
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
):
    bean_file_path = make_beanfile(
        """\
    option "inferred_tolerance_default" "JPY:1"
    """
    )
    output_dir = export_entries(bean_file_path)
    with open(output_dir / "option_maps.json", "rb") as fo:
        options = orjson.loads(fo.read())
        assert options["inferred_tolerance_default"] == {"JPY": "1"}


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


def test_transaction(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 open Assets:Cash
    1970-01-01 open Expenses:Grocery
    1970-01-02 * "Buy milk" "Wholefood"
        Assets:Cash     -5.99 USD
        Expenses:Grocery
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(
        output_dir / "transaction.bin",
        "COPY transaction FROM STDIN WITH (FORMAT BINARY)",
    )
    import_table(
        output_dir / "posting.bin",
        "COPY posting FROM STDIN WITH (FORMAT BINARY)",
    )
    transactions = db.query(Transaction).order_by(Transaction.date).all()
    assert len(transactions) == 1

    transaction0 = transactions[0]
    assert transaction0.date == datetime.date(1970, 1, 2)
    assert transaction0.entry_type == EntryType.TRANSACTION
    assert transaction0.flag == "*"
    assert transaction0.narration == "Wholefood"
    assert transaction0.payee == "Buy milk"

    assert len(transaction0.postings) == 2
    assert len(transaction0.postings) == db.query(Posting).count()
    postings = db.query(Posting).order_by(Posting.account).all()

    posting0 = postings[0]
    assert posting0.account == "Assets:Cash"
    assert posting0.units_number == decimal.Decimal("-5.99")
    assert posting0.units_currency == "USD"

    posting1 = postings[1]
    assert posting1.account == "Expenses:Grocery"
    assert posting1.units_number == decimal.Decimal("5.99")
    assert posting1.units_currency == "USD"


def test_transaction_with_price(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 open Assets:Cash
    1970-01-01 open Expenses:Grocery
    1970-01-02 * "Buy milk" "Wholefood"
        Assets:Cash     -1.0  BTC @ 123.45 USD
        Expenses:Grocery
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(
        output_dir / "transaction.bin",
        "COPY transaction FROM STDIN WITH (FORMAT BINARY)",
    )
    import_table(
        output_dir / "posting.bin",
        "COPY posting FROM STDIN WITH (FORMAT BINARY)",
    )
    transactions = db.query(Transaction).order_by(Transaction.date).all()
    assert len(transactions) == 1

    transaction0 = transactions[0]
    assert transaction0.date == datetime.date(1970, 1, 2)
    assert transaction0.entry_type == EntryType.TRANSACTION
    assert transaction0.flag == "*"
    assert transaction0.narration == "Wholefood"
    assert transaction0.payee == "Buy milk"

    assert len(transaction0.postings) == 2
    assert len(transaction0.postings) == db.query(Posting).count()
    postings = db.query(Posting).order_by(Posting.account).all()

    posting0 = postings[0]
    assert posting0.account == "Assets:Cash"
    assert posting0.units_number == decimal.Decimal("-1.0")
    assert posting0.units_currency == "BTC"
    assert posting0.price_number == decimal.Decimal("123.45")
    assert posting0.price_currency == "USD"

    posting1 = postings[1]
    assert posting1.account == "Expenses:Grocery"
    assert posting1.units_number == decimal.Decimal("123.45")
    assert posting1.units_currency == "USD"


def test_transaction_with_cost(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        """\
    1970-01-01 open Assets:Cash
    1970-01-01 open Assets:TSLA
    1970-01-01 open Income:PnL
    1970-01-02 * "Buy TSLA"
        Assets:TSLA     10.0  TSLA {123.45 USD, "ref-001"}
        Assets:Cash
    1970-01-03 * "Sell TSLA"
        Assets:TSLA     -5.0  TSLA {123.45 USD, 1970-01-02, "ref-001"} @ 200.00 USD
        Assets:Cash   1000.0 USD
        Income:PnL   -382.75 USD
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(
        output_dir / "transaction.bin",
        "COPY transaction FROM STDIN WITH (FORMAT BINARY)",
    )
    import_table(
        output_dir / "posting.bin",
        "COPY posting FROM STDIN WITH (FORMAT BINARY)",
    )
    transactions = db.query(Transaction).order_by(Transaction.date).all()
    assert len(transactions) == 2
    assert db.query(Posting).count() == 5

    transaction0 = transactions[0]
    assert transaction0.date == datetime.date(1970, 1, 2)
    assert transaction0.entry_type == EntryType.TRANSACTION
    assert transaction0.flag == "*"
    assert transaction0.narration == "Buy TSLA"
    assert len(transaction0.postings) == 2

    postings = (
        db.query(Posting)
        .filter_by(transaction=transaction0)
        .order_by(Posting.account)
        .all()
    )
    posting0 = postings[0]
    assert posting0.account == "Assets:Cash"
    assert posting0.units_number == decimal.Decimal("-1234.500")
    assert posting0.units_currency == "USD"

    posting1 = postings[1]
    assert posting1.account == "Assets:TSLA"
    assert posting1.units_number == decimal.Decimal("10")
    assert posting1.units_currency == "TSLA"
    assert posting1.cost_currency == "USD"
    assert posting1.cost_date == datetime.date(1970, 1, 2)
    assert posting1.cost_number == decimal.Decimal("123.45")
    assert posting1.cost_label == "ref-001"

    transaction1 = transactions[1]
    assert transaction1.date == datetime.date(1970, 1, 3)
    assert transaction1.entry_type == EntryType.TRANSACTION
    assert transaction1.flag == "*"
    assert transaction1.narration == "Sell TSLA"
    assert len(transaction1.postings) == 3

    postings = (
        db.query(Posting)
        .filter_by(transaction=transaction1)
        .order_by(Posting.account)
        .all()
    )
    posting0 = postings[0]
    assert posting0.account == "Assets:Cash"
    assert posting0.units_number == decimal.Decimal("1000")
    assert posting0.units_currency == "USD"

    posting1 = postings[1]
    assert posting1.account == "Assets:TSLA"
    assert posting1.units_number == decimal.Decimal("-5")
    assert posting1.units_currency == "TSLA"
    assert posting1.cost_currency == "USD"
    assert posting1.cost_date == datetime.date(1970, 1, 2)
    assert posting1.cost_number == decimal.Decimal("123.45")
    assert posting1.cost_label == "ref-001"

    posting2 = postings[2]
    assert posting2.account == "Income:PnL"
    assert posting2.units_number == decimal.Decimal("-382.75")
    assert posting2.units_currency == "USD"


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


def test_document(
    db: Session,
    tmp_path: pathlib.Path,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    invoice_pdf = tmp_path / "invoice.pdf"
    invoice_pdf.write_text("")
    bean_file_path = make_beanfile(
        f"""\
    1970-01-01 open Assets:Checking
    1970-01-02 document Assets:Checking "{invoice_pdf.name}"
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(
        output_dir / "document.bin", "COPY document FROM STDIN WITH (FORMAT BINARY)"
    )
    documents = db.query(Document).order_by(Document.date).all()
    assert len(documents) == 1

    document0 = documents[0]
    assert document0.date == datetime.date(1970, 1, 2)
    assert document0.entry_type == EntryType.DOCUMENT
    assert document0.account == "Assets:Checking"
    assert document0.filename == str(invoice_pdf.name)


def test_custom(
    db: Session,
    make_beanfile: MakeBeanfileFunc,
    export_entries: ExportEntriesFunc,
    import_table: ImportTableFunc,
):
    bean_file_path = make_beanfile(
        f"""\
    1970-01-01 custom "string val" 123.45 USD TRUE FALSE 2022-04-01 Assets:Bank "string"
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(
        output_dir / "custom.bin", "COPY custom FROM STDIN WITH (FORMAT BINARY)"
    )
    customs = db.query(Custom).order_by(Custom.date).all()
    assert len(customs) == 1

    custom0 = customs[0]
    assert custom0.date == datetime.date(1970, 1, 1)
    assert custom0.entry_type == EntryType.CUSTOM
    assert custom0.type == "string val"
    assert custom0.values == [
        '{"number": "123.45", "currency": "USD"}',
        "true",
        "false",
        "2022-04-01",
        "Assets:Bank",
        '"string"',
    ]
