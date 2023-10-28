"""Customer."""

import pandas as pd

from logic.schemas.base_schema import ABObject
from logic.schemas.customer.schema import CustomersSchema


# pylint: disable=too-few-public-methods
class CustomersObject(ABObject):
    """Customers object."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialize the object with a DataFrame containing customers output data.

        Args:
            df (pd.DataFrame): A dataframe instance of raw output file.
        """
        super().__init__(df)
        self._df = pd.DataFrame(CustomersSchema.validate(df))

    @property
    def df(self) -> pd.DataFrame:
        """Returns a copy of the internal DataFrame in order to avoid any external change to df.

        Note: Using .copy() can increase memory usage. Make sure you have enough RAM to handle
        the potential increase in memory consumption.

        Returns:
            pd.DataFrame: A copy of the internal DataFrame.
        """
        return self._df.copy()
