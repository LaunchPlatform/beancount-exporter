import os
import pathlib
import textwrap
import typing

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from .db.base import Base
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
    1970-01-01 open Equity:Opening-Balances
    """
    )
    output_dir = export_entries(bean_file_path)
    import_table(
        output_dir / "entry_base.bin", "COPY entry FROM STDIN WITH (FORMAT BINARY)"
    )
    import_table(output_dir / "open.bin", "COPY open FROM STDIN WITH (FORMAT BINARY)")

    opens = db.query(Open).all()
    assert db.query(Entry).count() == len(opens)
