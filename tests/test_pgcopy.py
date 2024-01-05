import os
import pathlib
import textwrap

import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy import text
from sqlalchemy.orm import Session

from beancount_exporter.main import main


@pytest.fixture
def engine() -> Engine:
    return create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)


@pytest.fixture
def db(engine: Engine) -> Session:
    session = Session(bind=engine)
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
    1970-01-01 custom "MOCK_TYPE" 123.45 USD TRUE "MOCK_STR_VALUE" 678.9

    """
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

    db.execute(
        text(
            """
        CREATE TEMP TABLE entry
        (
            id         uuid      not null
                primary key,
            entry_type entrytype not null,
            date       date      not null,
            meta       jsonb     not null
        ) ON COMMIT DROP
        """
        )
    )
    conn = db.connection()
    cursor = conn.connection.cursor()
    with open(output_dir / "base_entry.pgcopy.bin", "rb") as fo:
        cursor.copy_expert(
            """
                COPY
                entry ("id", "entry_type", "date", "meta")
                FROM STDIN WITH (FORMAT BINARY)
            """,
            file=fo,
        )
