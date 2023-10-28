"""Products Schema."""

import pandera as pa
from pandera import dtypes
from pandera.typing import Series


# pylint: disable=too-few-public-methods
class ProductsSchema(pa.SchemaModel):
    """Products Object Schema."""

    product_id: Series[dtypes.String] = pa.Field(nullable=False)
    product_category_name: Series[dtypes.String] = pa.Field(nullable=True)
    product_name_lenght: Series[dtypes.Float32] = pa.Field(nullable=True)
    product_description_lenght: Series[dtypes.Float32] = pa.Field(nullable=True)
    product_photos_qty: Series[dtypes.Float32] = pa.Field(nullable=True)
    product_weight_g: Series[dtypes.Float32] = pa.Field(nullable=True)
    product_length_cm: Series[dtypes.Float32] = pa.Field(nullable=True)
    product_height_cm: Series[dtypes.Float32] = pa.Field(nullable=True)
    product_width_cm: Series[dtypes.Float32] = pa.Field(nullable=True)

    class Config:
        """Configuration for Pandera schema.

        strict = False: Accept columns not listed above for product master merge.
        coerce = True: Convert dtype when possible.
        """

        strict = False
        coerce = True
