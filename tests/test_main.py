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
