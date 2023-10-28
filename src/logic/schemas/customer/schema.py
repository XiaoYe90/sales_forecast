"""Customers Schema."""

import pandera as pa
from pandera import dtypes
from pandera.typing import Series


# pylint: disable=too-few-public-methods
class CustomersSchema(pa.SchemaModel):
    """Customers Object Schema."""

    customer_id: Series[dtypes.String] = pa.Field(nullable=False)
    customer_unique_id: Series[dtypes.String] = pa.Field(nullable=False)
    customer_zip_code_prefix: Series[dtypes.Int64] = pa.Field(nullable=False)
    customer_city: Series[dtypes.String] = pa.Field(nullable=False)
    customer_state: Series[dtypes.String] = pa.Field(nullable=False)

    class Config:
        """Configuration for Pandera schema.

        strict = False: Accept columns not listed above for product master merge.
        coerce = True: Convert dtype when possible.
        """

        strict = False
        coerce = True
