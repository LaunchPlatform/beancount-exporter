import decimal
import enum
import functools
import json
import logging
import os
import pathlib
import sys
import typing

import click
from beancount import loader
from beancount.core import data
from beancount.ops import validation
from beancount_data import Balance
from beancount_data import Close
from beancount_data import Commodity
from beancount_data import Custom
from beancount_data import Document
from beancount_data import Event
from beancount_data import Note
from beancount_data import Open
from beancount_data import Pad
from beancount_data import Price
from beancount_data import Transaction
from beancount_data import ValidationError
from beancount_data import ValidationResult
from pydantic import BaseModel


ENTRY_TYPE_MODEL_MAP: typing.Dict[typing.Type, BaseModel] = {
    data.Open: Open,
    data.Close: Close,
    data.Commodity: Commodity,
    data.Pad: Pad,
    data.Balance: Balance,
    data.Transaction: Transaction,
    data.Note: Note,
    data.Event: Event,
    data.Price: Price,
    data.Document: Document,
    data.Custom: Custom,
}


def strip_base_path(
    paths: typing.Union[str, typing.List[str]], base_path: pathlib.Path
) -> typing.Union[str, typing.List[str]]:
    """Strip the base path off the given path

    :param paths: the path to strip off, could be a list of path or just one path
    :param base_path: the base path
    :return: path with base path stripped
    """
    if isinstance(paths, typing.List):
        return [str(pathlib.Path(path).relative_to(base_path)) for path in paths]
    else:
        path_value = pathlib.Path(paths)
        return str(path_value.relative_to(base_path))


@click.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "--base-path",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    default=os.getcwd(),
    envvar="BASE_PATH",
    help="Base path for stripping the file paths in the output",
)
@click.option(
    "--disable-path-stripping", is_flag=True, help="Disable stripping file path"
)
@click.option("--disable-options", is_flag=True, help="Disable options from the output")
@click.option(
    "--disable-validations",
    is_flag=True,
    help="Disable validation result from the output",
)
@click.option("--disable-entries", is_flag=True, help="Disable entries from the output")
def main(
    filename: str,
    base_path: click.Path,
    disable_path_stripping: bool,
    disable_options: bool,
    disable_validations: bool,
    disable_entries: bool,
):
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")

    entries, errors, options_map = loader.load_file(
        filename,
        log_timings=logging.info,
        log_errors=sys.stderr,
        extra_validations=validation.HARDCORE_VALIDATIONS,
    )

    strip_path = functools.partial(strip_base_path, base_path=pathlib.Path(base_path))

    options = options_map.copy()
    for key, value in options.items():
        if isinstance(value, set):
            options[key] = list(value)
        elif isinstance(value, decimal.Decimal):
            options[key] = str(value)
        elif isinstance(value, enum.Enum):
            options[key] = value.value
        if not disable_path_stripping:
            if key in {"filename", "include"}:
                options[key] = strip_path(value)
    del options["dcontext"]
    if not disable_options:
        print(json.dumps(options))
        print()
    if not disable_validations:
        validation_result = ValidationResult(
            errors=list(map(ValidationError.from_orm, errors))
        )
        for error in validation_result.errors:
            filename = error.source.get("filename")
            if filename is not None:
                error.source["filename"] = strip_path(filename)
            if error.entry is not None:
                if "filename" in error.entry.meta:
                    error.entry.meta["filename"] = strip_path(
                        error.entry.meta["filename"]
                    )
                if isinstance(error.entry, Transaction):
                    for posting in error.entry.postings:
                        posting_filename = posting.meta.get("filename")
                        if posting_filename is None:
                            continue
                        posting.meta["filename"] = strip_path(posting_filename)

        print(validation_result.json())
        print()
    if not disable_entries:
        for entry in entries:
            model_cls = ENTRY_TYPE_MODEL_MAP[type(entry)]
            model = model_cls.from_orm(entry)
            filename = model.meta.get("filename")
            if filename is not None:
                model.meta["filename"] = strip_path(filename)
            if isinstance(model, Document):
                model.filename = strip_path(model.filename)
            elif isinstance(model, Transaction):
                for posting in model.postings:
                    posting_filename = posting.meta.get("filename")
                    if posting_filename is None:
                        continue
                    posting.meta["filename"] = strip_path(posting_filename)
            print(model.json())
    sys.stdout.flush()
    exit(1 if errors else 0)


if __name__ == "__main__":
    main()
