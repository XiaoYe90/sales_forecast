"""AB Domain Object."""
import pandas as pd
import pandera as pa


# pylint: disable=too-few-public-methods
class ABDataFrameSchema(pa.DataFrameModel):
    """AB Object Data Frame Schema."""


# pylint: disable=too-few-public-methods
class ABObject:
    """AB data object."""

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialize the object with a DataFrame containing AB data.

        Args:
            df: A DataFrame containing AB data.
        """
        # pylint: disable=invalid-name
        self._df = pd.DataFrame(ABDataFrameSchema.validate(df))  # type: ignore

    @property
    def df(self) -> pd.DataFrame:
        """Returns a copy of the internal DataFrame in order to avoid any external change to df.

        Returns:
            pd.DataFrame: A copy of the internal DataFrame.
        """
        # pylint: disable=invalid-name
        df: pd.DataFrame = self._df.copy()
        return df
