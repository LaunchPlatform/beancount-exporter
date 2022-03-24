import logging
import sys

import click
from beancount import loader
from beancount.ops import validation

from beancount_data import ValidationError
from beancount_data import ValidationResult


@click.command()
@click.argument("filename", type=click.Path(exists=True))
def main(filename: str):
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")

    entries, errors, options_map = loader.load_file(
        filename,
        log_timings=logging.info,
        log_errors=sys.stderr,
        extra_validations=validation.HARDCORE_VALIDATIONS,
    )
    validation_result = ValidationResult(
        errors=list(map(ValidationError.from_orm, errors))
    )
    print(validation_result.json())
    sys.stdout.flush()
    exit(1 if errors else 0)


if __name__ == "__main__":
    main()
