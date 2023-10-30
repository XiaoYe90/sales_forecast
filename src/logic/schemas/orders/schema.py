"""Order Schema."""

import pandera as pa
from pandera import dtypes
from pandera.typing import Series


# pylint: disable=too-few-public-methods
class OrdersSchema(pa.SchemaModel):
    """Order Object Schema."""

    order_id: Series[dtypes.String] = pa.Field(nullable=False)
    customer_id: Series[dtypes.String] = pa.Field(nullable=False)
    order_status: Series[dtypes.String] = pa.Field(nullable=False)
    order_purchase_timestamp: Series[dtypes.DateTime] = pa.Field(nullable=False)
    order_approved_at: Series[dtypes.DateTime] = pa.Field(nullable=True)
    order_delivered_carrier_date: Series[dtypes.DateTime] = pa.Field(nullable=True)
    order_delivered_customer_date: Series[dtypes.DateTime] = pa.Field(nullable=True)
    order_estimated_delivery_date: Series[dtypes.DateTime] = pa.Field(nullable=False)

    class Config:
        """Configuration for Pandera schema.

        strict = False: Accept columns not listed above for product master merge.
        coerce = True: Convert dtype when possible.
        """

        strict = False
        coerce = True
