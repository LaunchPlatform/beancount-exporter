import json
import pathlib

import pytest
from click.testing import CliRunner

from beancount_exporter.main import main
from beancount_exporter.utils import strip_base_path


def test_strip_relative_path_under_base():
    base = pathlib.Path("/ledger")
    assert strip_base_path("/ledger/books/main.bean", base) == "books/main.bean"


def test_strip_path_equal_to_base():
    base = pathlib.Path("/ledger")
    assert strip_base_path("/ledger", base) == "."
    assert strip_base_path("/ledger/", base) == "."


def test_strip_non_relative_raises():
    base = pathlib.Path("/ledger")
    with pytest.raises(ValueError):
        strip_base_path("/other/main.bean", base)


def test_strip_escape_via_dotdot():
    # Same as pathlib.Path.relative_to: prefix matches, ".." segments remain.
    base = pathlib.Path("/ledger")
    assert strip_base_path("/ledger/../other/main.bean", base) == "../other/main.bean"


def test_strip_plugin_style_passthrough():
    base = pathlib.Path("/ledger")
    assert strip_base_path("<auto_accounts>", base) == "<auto_accounts>"
    assert strip_base_path("<pad>", base) == "<pad>"


def test_strip_list_of_paths():
    base = pathlib.Path("/ledger")
    assert strip_base_path(
        ["/ledger/a.bean", "/ledger/sub/b.bean", "<plugin>"],
        base,
    ) == ["a.bean", "sub/b.bean", "<plugin>"]


def test_strip_cache_reuses_repeated_paths():
    base = pathlib.Path("/ledger")
    path = "/ledger/main.bean"
    cache: dict[str, str] = {}
    assert strip_base_path(path, base, cache=cache) == "main.bean"
    assert cache[path] == "main.bean"

    # Mutate the cache entry to prove a subsequent call hits the cache.
    cache[path] = "FROM_CACHE"
    assert strip_base_path(path, base, cache=cache) == "FROM_CACHE"


def test_disable_path_stripping_leaves_meta_filenames(tmp_path: pathlib.Path):
    bean_file_path = tmp_path / "main.bean"
    bean_file_path.write_text(
        "1970-01-01 open Assets:Checking USD\n"
        "1970-01-01 open Equity:Opening-Balances\n"
        '1970-01-01 * "Txn"\n'
        "  Assets:Checking             1.00 USD\n"
        "  Equity:Opening-Balances\n"
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            str(bean_file_path),
            "--base-path",
            str(tmp_path),
            "--disable-path-stripping",
            "--disable-validations",
            "--disable-options",
        ],
    )
    assert result.exit_code == 0
    assert not result.exception

    entries = [
        json.loads(line) for line in result.output.strip().split("\n") if line.strip()
    ]
    filenames = [entry["meta"]["filename"] for entry in entries]
    assert filenames
    assert all(filename == str(bean_file_path) for filename in filenames)
