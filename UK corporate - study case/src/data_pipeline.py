"""
ETL Module for Loading and Writing UK Corporate Data.

This module performs Extract, Transform, Load (ETL) operations for handling
UK corporate data, including companies, filings, and officers/owners datasets.
The module uses CSV files as input, loads them into Polars DataFrames, and writes
the processed data to Parquet files for efficient storage.

Future Improvements:
---------------------
1. Dynamically set the base path using `pathlib.Path.cwd()` instead of hardcoding.
2. Add error handling for missing or corrupted input files.
3. Allow configuration of paths and settings via a configuration file or environment variables.
4. Improve logging granularity for better debugging and traceability.
"""
import pathlib
import logging
from polars import DataFrame as pl_df

import etl_tools
import etl_logger

BASE_PATH = pathlib.Path(__file__).resolve().parent.parent

# Define specific data directories
COMPANIES_DATA = BASE_PATH / 'data' / 'companies'
FILINGS_DATA = BASE_PATH / 'data' / 'filings'
OFFICE_OWNERS_DATA = BASE_PATH / 'data' / 'officers_and_owners'

# Get logger
console = logging.StreamHandler()
logger = etl_logger.get_logger('logger', logging.WARNING, [console])

# Load data
companies: pl_df = etl_tools.load_file(
    logger, COMPANIES_DATA.with_suffix('.csv'), separator=';')
filings: pl_df = etl_tools.load_file(
    logger, FILINGS_DATA.with_suffix('.csv'), separator=';')
officers_owners: pl_df = etl_tools.load_file(
    logger, OFFICE_OWNERS_DATA.with_suffix('.csv'), separator=';')


# # Write parquet
etl_tools.write_parquet(logger,
                        companies,
                        COMPANIES_DATA.with_suffix('.parquet'),
                        compression_level=22)
etl_tools.write_parquet(logger,
                        filings,
                        FILINGS_DATA.with_suffix('.parquet'),
                        compression_level=22)
etl_tools.write_parquet(logger,
                        officers_owners,
                        OFFICE_OWNERS_DATA.with_suffix('.parquet'),
                        compression_level=22)