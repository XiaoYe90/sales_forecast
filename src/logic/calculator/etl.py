"""Implements a sales ETL pipeline (extract, transform, load) for processing sales data."""

import os
import shutil
from typing import List, Optional, Type

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from logic.schemas.base_schema import ABObject
from logic.schemas.customer.customer import CustomersObject
from logic.schemas.customer.schema import CustomersSchema
from logic.schemas.order_items.order_items import OrderItemsObject
from logic.schemas.order_items.schema import OrderItemsSchema
from logic.schemas.order_reviews.order_reviews import OrderReviewsObject
from logic.schemas.order_reviews.schema import OrderReviewsSchema
from logic.schemas.orders.orders import OrdersObject
from logic.schemas.orders.schema import OrdersSchema


class DataCalculator:
    """Class for sales forecast calculation."""

    def __init__(self, csv_directory: str) -> None:
        """Initialize DataCalculator and read the necessary CSV files.

        Args:
            csv_directory (str): Directory containing the CSV files.
        """
        self._read_files(csv_directory)
        self._output: pd.DataFrame = pd.DataFrame()

    def _read_files(self, csv_directory: str) -> None:
        """Read the necessary CSV files and create objects.

        Args:
            csv_directory (str): Directory containing the CSV files.
        """
        self._object_map = {
            "customers": self._read_file_and_create_object(
                os.path.join(csv_directory, "olist_customers_dataset.csv"),
                CustomersObject,
            ),
            "order_items": self._read_file_and_create_object(
                os.path.join(csv_directory, "olist_order_items_dataset.csv"),
                OrderItemsObject,
            ),
            "orders": self._read_file_and_create_object(
                os.path.join(csv_directory, "olist_orders_dataset.csv"), OrdersObject
            ),
            "order_reviews": self._read_file_and_create_object(
                os.path.join(csv_directory, "olist_order_reviews_dataset.csv"),
                OrderReviewsObject,
            ),
        }

    def _read_file_and_create_object(self, file_path: str, object_class: Type) -> ABObject:
        """Read the CSV file and return a new object of the specified class.

        Args:
            file_path (str): Path to the CSV file.
            object_class (Type): Class to instantiate with the DataFrame.

        Returns:
            Type: Instantiated object_class object.
        """
        file_df = pd.read_csv(file_path)
        return object_class(file_df)

    @property
    def customers_object(self) -> pd.DataFrame:
        """Getter for customers_object DataFrame.

        Returns:
            pd.DataFrame: DataFrame of customers data.
        """
        return self._object_map["customers"].df

    @property
    def order_items_object(self) -> pd.DataFrame:
        """Getter for order_items_object DataFrame.

        Returns:
            pd.DataFrame: DataFrame of order items data.
        """
        return self._object_map["order_items"].df

    @property
    def order_reviews_object(self) -> pd.DataFrame:
        """Getter for order_reviews_object DataFrame.

        Returns:
            pd.DataFrame: DataFrame of order reviews data.
        """
        return self._object_map["order_reviews"].df

    @property
    def orders_object(self) -> pd.DataFrame:
        """Getter for orders_object DataFrame.

        Returns:
            pd.DataFrame: DataFrame of orders data.
        """
        return self._object_map["orders"].df

    @property
    def get_output(self) -> pd.DataFrame:
        """Getter for output DataFrame.

        Returns:
            pd.DataFrame: DataFrame of the output.
        """
        return self._output.copy()

    def calculate_index(self, product_list: Optional[List[str]] = None) -> None:
        """Calculate the dataset for modeling.

        Args:
            product_list (Optional[List[str]], optional):
                List of product IDs to filter the dataset on. Defaults to None.
        """
        summary_df = self.calculate_summary(product_list=product_list)
        product_ratings_df = self.calculate_product_ratings(product_list=product_list)
        product_sales_by_city_df = self.calculate_product_sales_by_city(
            product_list=product_list
        )

        # Merge the DataFrames into a single output DataFrame
        output_df = (
            summary_df.merge(product_ratings_df, on="product_id", how="outer")
            .merge(product_sales_by_city_df, on="product_id", how="outer")
            .fillna(0)
        )

        output_df["order_purchase_week"] = output_df["order_purchase_week"].astype(str)
        # Set the output DataFrame as a property of the object
        self._output = output_df

    def _filter_df_on_product_list(
        self, input_df: pd.DataFrame, product_list: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Filter a DataFrame based on the provided product_list.

        Args:
            df (pd.DataFrame): DataFrame to filter.
            product_list (Optional[List[str]], optional):
                List of product IDs to filter the DataFrame on. Defaults to None.

        Returns:
            pd.DataFrame: Filtered DataFrame.
        """
        if product_list and len(product_list) > 0:
            return input_df[input_df["product_id"].isin(product_list)]
        return input_df

    def calculate_summary(
        self, product_list: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Calculate the summary data for products.

        Args:
            product_list (Optional[List[str]], optional):
                List of product IDs to filter the dataset on. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing the summary data.
        """
        merged_df = self.orders_object.merge(
            self.order_items_object, on=OrderItemsSchema.order_id
        )

        # Parse date columns
        merged_df["order_purchase_datetime"] = pd.to_datetime(
            merged_df["order_purchase_timestamp"]
        )

        grouped_sales = (
            merged_df.groupby(
                ["product_id", pd.Grouper(key="order_purchase_datetime", freq="W")]
            )["price"]
            .agg(["sum", "count"])
            .reset_index()
        )
        grouped_sales["order_purchase_datetime"] = (
            grouped_sales["order_purchase_datetime"]
            .dt.strftime("%Y-%m-%d")
            .replace("NaT", "unknown")
        )
        grouped_sales = grouped_sales.rename(
            columns={"order_purchase_datetime": "order_purchase_week"}
        )
        if product_list and len(product_list) > 0:
            grouped_sales = self._filter_df_on_product_list(grouped_sales, product_list)
        return grouped_sales

    def calculate_product_ratings(
        self, product_list: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Calculate the product ratings data.

        Args:
            product_list (Optional[List[str]], optional):
                List of product IDs to filter the dataset on. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing the product ratings data.
        """
        # Merge orders_object and order_reviews_object on 'order_id'
        orders_reviews_merged = self.orders_object.merge(
            self.order_reviews_object, on=OrdersSchema.order_id
        ).merge(self.order_items_object, on=OrderItemsSchema.order_id)

        # Calculate the mean product rating for each product_id
        product_ratings = (
            orders_reviews_merged.groupby("product_id")[OrderReviewsSchema.review_score]
            .mean()
            .reset_index()
            .rename(columns={OrderReviewsSchema.review_score: "mean_product_rating"})
        )
        if product_list and len(product_list) > 0:
            product_ratings = self._filter_df_on_product_list(
                product_ratings, product_list
            )
        return product_ratings

    def calculate_product_sales_by_city(
        self, product_list: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Calculate the product sales by city data.

        Args:
            product_list (Optional[List[str]], optional):
                List of product IDs to filter the dataset on. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing the product sales by city data.
        """
        # Merge orders_object and customers_object on 'customer_id'
        orders_customers_merged = self.orders_object.merge(
            self.customers_object, on=CustomersSchema.customer_id
        ).merge(self.order_items_object, on=OrderItemsSchema.order_id)

        # Calculate sales count for each product_id and customer_city
        product_city_sales = (
            orders_customers_merged.groupby(
                ["product_id", CustomersSchema.customer_city]
            )["order_id"]
            .count()
            .reset_index()
            .rename(columns={"order_id": "sales_count"})
        )
        # Sort by product_id and sales_count, then groupby product_id
        sorted_product_city_sales = product_city_sales.sort_values(
            by=["product_id", "sales_count"], ascending=[True, False]
        )
        top_3_selling_cities = (
            sorted_product_city_sales.groupby("product_id")
            .head(3)
            .reset_index(drop=True)
        )

        # Group the top 3 selling cities by product_id into lists
        grouped_top_3_selling_cities = (
            top_3_selling_cities.groupby("product_id")[CustomersSchema.customer_city]
            .apply(list)
            .reset_index()
        )
        if product_list and len(product_list) > 0:
            grouped_top_3_selling_cities = self._filter_df_on_product_list(
                grouped_top_3_selling_cities, product_list
            )
        return grouped_top_3_selling_cities

    def save_output_to_parquet(self, output_directory: str) -> None:
        """Save the output to Parquet format.

        Args:
            output_directory (str): Directory to save the Parquet output.
        """
        # Clean the output directory
        for item in os.listdir(output_directory):
            item_path = os.path.join(output_directory, item)
            if os.path.isfile(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)

        unique_product_ids = self.get_output["product_id"].unique()
        num_partitions = max(1, len(unique_product_ids) // 1024)

        for partition_index in range(num_partitions):
            start_index = partition_index * 1024
            end_index = (partition_index + 1) * 1024
            product_ids_partition = unique_product_ids[start_index:end_index]
            output_df_partition = self.get_output[
                self.get_output["product_id"].isin(product_ids_partition)
            ]

            # Create a directory for partition
            partition_dir = os.path.join(
                output_directory, f"partition_{partition_index}"
            )
            os.makedirs(partition_dir, exist_ok=True)

            table = pa.Table.from_pandas(output_df_partition)
            pq.write_to_dataset(
                table, root_path=partition_dir, partition_cols=["product_id"]
            )


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(os.path.dirname(script_dir))

    csv_dir = os.path.join(parent_dir, "data", "input")
    output_dir = os.path.join(parent_dir, "data", "output", "output_table")

    data_object = DataCalculator(csv_dir)
    data_object.calculate_index()
    data_object.save_output_to_parquet(output_dir)
