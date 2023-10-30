"""Geolocation Schema."""

import pandera as pa
from pandera import dtypes
from pandera.typing import Series


# pylint: disable=too-few-public-methods
class GeolocationSchema(pa.SchemaModel):
    """Geolocation Object Schema."""

    geolocation_zip_code_prefix: Series[dtypes.Int64] = pa.Field(nullable=False)
    geolocation_lat: Series[dtypes.Float64] = pa.Field(nullable=False)
    geolocation_lng: Series[dtypes.Float64] = pa.Field(nullable=False)
    geolocation_city: Series[dtypes.String] = pa.Field(nullable=False)
    geolocation_state: Series[dtypes.String] = pa.Field(nullable=False)

    class Config:
        """Configuration for Pandera schema.

        strict = False: Accept columns not listed above for product master merge.
        coerce = True: Convert dtype when possible.
        """

        strict = False
        coerce = True
