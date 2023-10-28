"""Order items."""

import pandas as pd

from logic.schemas.base_schema import ABObject
from logic.schemas.order_items.schema import OrderItemsSchema


# pylint: disable=too-few-public-methods
class OrderItemsObject(ABObject):
    """Orders items object."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialize the object with a DataFrame containing orders items output data.

        Args:
            df (pd.DataFrame): A dataframe instance of raw output file.
        """
        super().__init__(df)
        self._df = pd.DataFrame(OrderItemsSchema.validate(df))

    @property
    def df(self) -> pd.DataFrame:
        """Returns a copy of the internal DataFrame in order to avoid any external change to df.

        Returns:
            pd.DataFrame: A copy of the internal DataFrame.
        """
        return self._df.copy()
