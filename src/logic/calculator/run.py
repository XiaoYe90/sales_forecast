"""Main module to run the sales ETL pipeline."""

import argparse
import os
import sys

import yaml

from logic.calculator.etl import DataCalculator


def main(main_args) -> None:
    """Execute the main function and run the sales ETL pipeline.

    Args:
        main_args (argparse.Namespace): Command line arguments.
    """
    with open(main_args.config, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    parent_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.realpath(sys.argv[0])))
    )

    csv_dir = config.get("csv_dir", "data/input")
    output_dir = config.get("output_dir", "data/output/output_table")
    if not os.path.isabs(csv_dir):
        csv_dir = os.path.join(parent_dir, csv_dir)
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(parent_dir, output_dir)

    product_list = config.get("product_list")

    data_object = DataCalculator(csv_dir)
    data_object.calculate_index(product_list=product_list)
    data_object.save_output_to_parquet(output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the sales ETL pipeline")
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="config.yml",
        help="Path to the configuration file",
    )
    args = parser.parse_args()

    main(args)
