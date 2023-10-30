"""Order Reviews Schema."""

import pandera as pa
from pandera import dtypes
from pandera.typing import Series


# pylint: disable=too-few-public-methods
class OrderReviewsSchema(pa.SchemaModel):
    """Order Reviews Object Schema."""

    review_id: Series[dtypes.String] = pa.Field(nullable=False)
    order_id: Series[dtypes.String] = pa.Field(nullable=False)
    review_score: Series[dtypes.Int64] = pa.Field(nullable=False)
    review_comment_title: Series[dtypes.String] = pa.Field(nullable=True)
    review_comment_message: Series[dtypes.String] = pa.Field(nullable=True)
    review_creation_date: Series[dtypes.DateTime] = pa.Field(nullable=False)
    review_answer_timestamp: Series[dtypes.DateTime] = pa.Field(nullable=False)

    class Config:
        """Configuration for Pandera schema.

        strict = False: Accept columns not listed above for product master merge.
        coerce = True: Convert dtype when possible.
        """

        strict = False
        coerce = True
