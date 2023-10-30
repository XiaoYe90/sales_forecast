"""Product category."""

import pandas as pd

from logic.schemas.base_schema import ABObject
from logic.schemas.product_category.schema import ProductCategorySchema


# pylint: disable=too-few-public-methods
class ProductCategoryObject(ABObject):
    """Products category object."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialize the object with a DataFrame containing products category output data.

        Args:
            df (pd.DataFrame): A dataframe instance of raw output file.
        """
        super().__init__(df)
        self._df = pd.DataFrame(ProductCategorySchema.validate(df))

    @property
    def df(self) -> pd.DataFrame:
        """Returns a copy of the internal DataFrame in order to avoid any external change to df.

        Note: Using .copy() can increase memory usage. Make sure you have enough RAM to handle
        the potential increase in memory consumption.

        Returns:
            pd.DataFrame: A copy of the internal DataFrame.
        """
        return self._df.copy()
