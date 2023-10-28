"""Product Category Schema."""

import pandera as pa
from pandera import dtypes
from pandera.typing import Series


# pylint: disable=too-few-public-methods
class ProductCategorySchema(pa.SchemaModel):
    """Product Category Object Schema."""

    product_category_name: Series[dtypes.String] = pa.Field(nullable=True)
    product_category_name_english: Series[dtypes.String] = pa.Field(nullable=True)

    class Config:
        """Configuration for Pandera schema.

        strict = False: Accept columns not listed above for product master merge.
        coerce = True: Convert dtype when possible.
        """

        strict = False
        coerce = True
