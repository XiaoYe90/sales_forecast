"""Implements a sales ETL pipeline (extract, transform, load) for processing sales data."""

import os
import shutil
from collections import defaultdict
from typing import Dict, List, Optional, Type

import dask.dataframe as dd
import pandas as pd

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

    def _read_file_and_create_object(
        self, file_path: str, object_class: Type
    ) -> ABObject:
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

        # Merge the DataFrames into a single output DataFrame
        output_df = summary_df.merge(
            product_ratings_df, on="product_id", how="outer"
        ).fillna(0)

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

    def _merge_sales_dicts(self, dicts: pd.Series) -> Dict[str, Dict[str, int]]:
        """Merges a series of sales dictionaries into a single dictionary.

        Args:
            dicts (pd.Series): A Pandas Series containing sales dictionaries.

        Returns:
            dict: The merged dictionary containing city as keys
                and {"sales_count": x, "sales_sum": y} as values.
        """
        merged_data = defaultdict(lambda: defaultdict(int))
        for cur_dict in dicts:
            for city, sales_data in cur_dict.items():
                merged_data[city]["sales_count"] += sales_data["sales_count"]
                merged_data[city]["sales_sum"] += sales_data["sales_sum"]
        return dict(merged_data)

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
            self.customers_object, on=CustomersSchema.customer_id
        ).merge(self.order_items_object, on=OrderItemsSchema.order_id)

        # Parse date columns
        merged_df["order_purchase_datetime"] = pd.to_datetime(
            merged_df["order_purchase_timestamp"]
        )

        grouped_sales = (
            merged_df.groupby(
                [
                    "product_id",
                    CustomersSchema.customer_city,
                    pd.Grouper(key="order_purchase_datetime", freq="W"),
                ]
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
        grouped_sales["city_sales"] = grouped_sales.apply(
            lambda row: {
                row["customer_city"]: {
                    "sales_count": row["count"],
                    "sales_sum": row["sum"],
                }
            },
            axis=1,
        )

        summary_df = (
            grouped_sales.groupby(["product_id", "order_purchase_week"])
            .agg(
                total_count=("count", "sum"),
                total_sales_sum=("sum", "sum"),
                city_sales=("city_sales", self._merge_sales_dicts),
            )
            .reset_index()
        )

        if product_list and len(product_list) > 0:
            summary_df = self._filter_df_on_product_list(summary_df, product_list)
        return summary_df

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

        # Convert the city_sales column to a string
        output_data = self.get_output
        output_data["city_sales"] = output_data["city_sales"].apply(str)

        # Convert the Pandas DataFrame to a Dask DataFrame
        dask_output_data = dd.from_pandas(output_data, npartitions=10)

        # Save the Dask DataFrame as Parquet files, partitioned by product_id
        dask_output_data.to_parquet(
            output_directory, engine="fastparquet", partition_on="product_id"
        )


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(os.path.dirname(script_dir))

    csv_dir = os.path.join(parent_dir, "data", "input")
    output_dir = os.path.join(parent_dir, "data", "output", "output_table")

    data_object = DataCalculator(csv_dir)
    data_object.calculate_index()
    data_object.save_output_to_parquet(output_dir)
