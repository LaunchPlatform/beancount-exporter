import logging
import os
import pathlib
import sys

import click
from beancount import loader
from beancount.ops import validation

from .formats.json_processor import JsonProcessor


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

    processor = JsonProcessor(base_path=pathlib.Path(str(base_path)))
    options = options_map.copy()
    for key, value in options.items():
        if not disable_path_stripping:
            if key in {"filename", "include"}:
                options[key] = processor.strip_path(value)
    del options["dcontext"]
    if not disable_options:
        processor.process_options(options)
    if not disable_validations:
        processor.process_errors(errors)
    if not disable_entries:
        processor.process_entries(entries)

    sys.stdout.flush()
    exit(1 if errors else 0)


if __name__ == "__main__":
    main()
