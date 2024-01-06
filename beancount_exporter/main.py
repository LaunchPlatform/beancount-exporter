import contextlib
import enum
import logging
import os
import pathlib
import sys

import click
from beancount import loader
from beancount.ops import validation

from .formats.json_processor import JsonProcessor
from .formats.pgcopy_processor import PgCopyProcessor
from .formats.pgcopy_processor.configs import ENTRY_TYPE_CONFIGS


@enum.unique
class ExportFormat(enum.StrEnum):
    JSON = "JSON"
    PGCOPY = "PGCOPY"


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
    "-f",
    "--format",
    type=click.Choice(ExportFormat),
    default=ExportFormat.JSON,
    help="Output format type",
)
@click.option(
    "--output-dir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    default=os.getcwd(),
    envvar="OUTPUT_DIR",
    help="Path for file-based output",
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
    output_dir: click.Path,
    format: ExportFormat,
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

    with contextlib.ExitStack() as stack:
        if format == ExportFormat.JSON:
            processor = JsonProcessor(base_path=pathlib.Path(str(base_path)))
        elif format == ExportFormat.PGCOPY:
            output_dir_path = pathlib.Path(str(output_dir))
            processor = PgCopyProcessor(
                base_path=pathlib.Path(str(base_path)),
                option_maps_file=stack.enter_context(
                    open(output_dir_path / "option_maps.json", "wb")
                ),
                errors_file=stack.enter_context(
                    open(output_dir_path / "errors.json", "wb")
                ),
                entry_base_file=stack.enter_context(
                    open(output_dir_path / "entry_base.bin", "wb")
                ),
                posting_file=stack.enter_context(
                    open(output_dir_path / "posting.bin", "wb")
                ),
                entry_files={
                    entry_type: stack.enter_context(
                        open(output_dir_path / f"{config.type.value}.bin", "wb")
                    )
                    for entry_type, config in ENTRY_TYPE_CONFIGS.items()
                },
            )
        processor.start()
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
        processor.stop()

    exit(1 if errors else 0)


if __name__ == "__main__":
    main()
