"""Products."""

import pandas as pd

from logic.schemas.base_schema import ABObject
from logic.schemas.products.schema import ProductsSchema


# pylint: disable=too-few-public-methods
class ProductsObject(ABObject):
    """Products object."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialize the object with a DataFrame containing products output data.

        Args:
            df (pd.DataFrame): A dataframe instance of raw output file.
        """
        super().__init__(df)
        self._df = pd.DataFrame(ProductsSchema.validate(df))

    @property
    def df(self) -> pd.DataFrame:
        """Returns a copy of the internal DataFrame in order to avoid any external change to df.

        Returns:
            pd.DataFrame: A copy of the internal DataFrame.
        """
        return self._df.copy()
