import json
import pathlib

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
