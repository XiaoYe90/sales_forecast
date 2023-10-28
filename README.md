# sales_forecast

This project implements a sales forecast logic for processing sales data.

## File Structure

- `src/logic/calculator/etl.py`: The main logic file containing the ETL pipeline implementation, including the DataCalculator class and related functions.
- `src/logic/calculator/run.py`: The executable file that demonstrates how to use the DataCalculator class to process sales data and save the output to a desired format (e.g., Parquet).
- `src/logic/calculator/config.yml`: The configuration file for the ETL pipeline, containing settings such as country codes, data path, and schema configuration.
- `src/logic/test/test_etl.py`: Unit tests for the ETL pipeline implementation, ensuring the correct functionality of the logic.
- `src/logic/schemas`: Folder to hold the schema definition scripts.
- `src/data`: Folder to store the input and output data.

## Usage

To process the sales data, follow these steps:

1. Configure the `config.yml` file with the appropriate settings for the desired country and data sources.
2. Run `run.py` to execute the ETL pipeline and process the data.
3. Check the output in the specified output directory.

For additional information and detailed usage instructions, refer to the comments and documentation within each file.

## Tests

To execute the unit tests, run `src/logic/test_etl.py`. This will help verify the correct functionality of the ETL pipeline and ensure that any future modifications do not introduce errors.

## Running scripts locally

```shell
export PYTHONPATH="src"
```