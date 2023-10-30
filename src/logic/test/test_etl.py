# pylint: disable=redefined-outer-name

"""ETL calculator test."""

import os
import shutil
from typing import Tuple

import pandas as pd
import pytest

from logic.calculator.etl import DataCalculator


@pytest.fixture(scope="module")
def setup_calculator(tmpdir_factory: tuple) -> Tuple:
    """Fixture to setup a DataCalculator instance and a temporary output directory.

    Args:
        tmp_path_factory (pytest.TempPathFactory): Pytest temporary path factory.

    Returns:
        tuple: Contains the DataCalculator instance and the output directory path.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(os.path.dirname(script_dir))
    csv_dir = os.path.join(parent_dir, "data", "input")
    output_dir = str(tmpdir_factory.mktemp("temp_output"))

    data_object = DataCalculator(csv_dir)
    yield data_object, output_dir

    # Clean up the output directory
    shutil.rmtree(output_dir)


def test_calculate_summary(setup_calculator: tuple) -> None:
    """Test the calculate_summary() method for DataCalculator."""
    data_object, _ = setup_calculator
    product_list = ["4244733e06e7ecb4970a6e2683c13e61"]

    # Test calculate_summary() without product_list
    result_no_product_list = data_object.calculate_summary()
    assert isinstance(result_no_product_list, pd.DataFrame)

    # Test calculate_summary() with product_list
    result_with_product_list = data_object.calculate_summary(product_list=product_list)
    assert isinstance(result_with_product_list, pd.DataFrame)
    assert all(result_with_product_list["product_id"].isin(product_list))


def test_calculate_product_ratings(setup_calculator: tuple) -> None:
    """Test the calculate_product_ratings() method for DataCalculator."""
    data_object, _ = setup_calculator
    product_list = ["4244733e06e7ecb4970a6e2683c13e61"]

    # Test calculate_product_ratings() without product_list
    result_no_product_list = data_object.calculate_product_ratings()
    assert isinstance(result_no_product_list, pd.DataFrame)

    # Test calculate_product_ratings() with product_list
    result_with_product_list = data_object.calculate_product_ratings(
        product_list=product_list
    )
    assert isinstance(result_with_product_list, pd.DataFrame)
    assert all(result_with_product_list["product_id"].isin(product_list))
