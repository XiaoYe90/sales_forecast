"""Order Items Schema."""

import pandera as pa
from pandera import dtypes
from pandera.typing import Series


# pylint: disable=too-few-public-methods
class OrderItemsSchema(pa.SchemaModel):
    """Order Items Object Schema."""

    order_id: Series[dtypes.String] = pa.Field(nullable=False)
    order_item_id: Series[dtypes.Int64] = pa.Field(nullable=False)
    product_id: Series[dtypes.String] = pa.Field(nullable=False)
    seller_id: Series[dtypes.String] = pa.Field(nullable=False)
    shipping_limit_date: Series[dtypes.DateTime] = pa.Field(nullable=False)
    price: Series[dtypes.Float64] = pa.Field(nullable=False)
    freight_value: Series[dtypes.Float64] = pa.Field(nullable=False)

    class Config:
        """Configuration for Pandera schema.

        strict = False: Accept columns not listed above for product master merge.
        coerce = True: Convert dtype when possible.
        """

        strict = False
        coerce = True
