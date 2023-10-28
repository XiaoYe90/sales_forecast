"""Order Payments Schema."""

import pandera as pa
from pandera import dtypes
from pandera.typing import Series


# pylint: disable=too-few-public-methods
class OrderPaymentsSchema(pa.SchemaModel):
    """Order Payments Object Schema."""

    order_id: Series[dtypes.String] = pa.Field(nullable=False)
    payment_sequential: Series[dtypes.Int64] = pa.Field(nullable=False)
    payment_type: Series[dtypes.String] = pa.Field(nullable=False)
    payment_installments: Series[dtypes.Int64] = pa.Field(nullable=False)
    payment_value: Series[dtypes.Float64] = pa.Field(nullable=False)

    class Config:
        """Configuration for Pandera schema.

        strict = False: Accept columns not listed above for product master merge.
        coerce = True: Convert dtype when possible.
        """

        strict = False
        coerce = True
