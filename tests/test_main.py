import json
import pathlib
import textwrap

from click.testing import CliRunner

from beancount_exporter.main import main


def test_nested_decimal(tmp_path: pathlib.Path):
    bean_file_path = tmp_path / "main.bean"
    bean_file_path.write_text('option "inferred_tolerance_default" "JPY:1"')

    runner = CliRunner()
    result = runner.invoke(
        main,
        [str(bean_file_path), "--base-path", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert not result.exception
    parts = result.output.split("\n")
    options = json.loads(parts[0])
    assert options["inferred_tolerance_default"] == {"JPY": "1"}


def test_filepath_of_plugin_generated_entries(tmp_path: pathlib.Path):
    bean_file_path = tmp_path / "main.bean"
    bean_file_path.write_text(
        textwrap.dedent(
            """\
    plugin "beancount.plugins.auto_accounts"

    2015-02-01 * "Aqua Viva Tulum - two nights"
        Income:Caroline:CreditCard      -269.00 USD
        Expenses:Accommodation
    """
        )
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [str(bean_file_path), "--base-path", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert not result.exception
    parts = result.output.split("\n")
    entries = list(map(json.loads, parts[4:-1]))
    assert [entry["meta"]["filename"] for entry in entries] == [
        "<auto_accounts>",
        "<auto_accounts>",
        "main.bean",
    ]


def test_txn_entries_generated_from_pad(tmp_path: pathlib.Path):
    bean_file_path = tmp_path / "main.bean"
    bean_file_path.write_text(
        textwrap.dedent(
            """\

    1970-01-01 open Assets:Checking
    1970-01-01 open Equity:Opening-Balances
    2023-02-10 pad Assets:Checking Equity:Opening-Balances
    2023-03-22 balance Assets:Checking 123.45 USD
    
    """
        )
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [str(bean_file_path), "--base-path", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert not result.exception
    parts = result.output.split("\n")
    entries = list(map(json.loads, parts[4:-1]))
    padding_txn = entries[3]
    assert padding_txn["entry_type"] == "transaction"
    assert padding_txn["flag"] == "P"
    assert (
        padding_txn["narration"]
        == "(Padding inserted for Balance of 123.45 USD for difference 123.45 USD)"
    )
    assert len(padding_txn["postings"]) == 2
    assert padding_txn["postings"][0]["account"] == "Assets:Checking"
    assert padding_txn["postings"][0]["units"]["number"] == 123.45
    assert padding_txn["postings"][0]["units"]["currency"] == "USD"
    assert padding_txn["postings"][1]["account"] == "Equity:Opening-Balances"
    assert padding_txn["postings"][1]["units"]["number"] == -123.45
    assert padding_txn["postings"][1]["units"]["currency"] == "USD"


def test_custom_values(tmp_path: pathlib.Path):
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
        [str(bean_file_path), "--base-path", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert not result.exception
    parts = result.output.split("\n")
    entries = list(map(json.loads, parts[4:-1]))
    assert len(entries)
    custom = entries[0]
    assert custom["date"] == "1970-01-01"
    assert custom["entry_type"] == "custom"
    assert custom["type"] == "MOCK_TYPE"
    assert custom["values"] == [
        {"number": "123.45", "currency": "USD"},
        True,
        "MOCK_STR_VALUE",
        "678.9",
    ]
