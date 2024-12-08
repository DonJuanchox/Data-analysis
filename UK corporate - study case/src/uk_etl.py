import etl_tools
import pathlib
import etl_logger
import logging
from polars import DataFrame as pl_df

#  TODO> remember this to change and use cwd
# Extract
path = pathlib.Path(
    r'C:\Users\juann\OneDrive\Documentos\GitHub\Data-analysis\UK corporate - study case\data')
companies_data = path.joinpath('companies')
filings_data = path.joinpath('filings')
officers_owners_data = path.joinpath('officers_and_owners')

# Get logger
console = logging.StreamHandler()
logger = etl_logger.get_logger('logger', logging.WARNING, [console])

# Load data
companies: pl_df = etl_tools.load_file(
    logger, companies_data.with_suffix('.csv'), separator=';')
filings: pl_df = etl_tools.load_file(
    logger, filings_data.with_suffix('.csv'), separator=';')
officers_owners: pl_df = etl_tools.load_file(
    logger, officers_owners_data.with_suffix('.csv'), separator=';')


# # Write parquet
etl_tools.write_parquet(
    logger, companies, companies_data.with_suffix('.parquet'), compression_level=22)
etl_tools.write_parquet(
    logger, filings, filings_data.with_suffix('.parquet'), compression_level=22)
etl_tools.write_parquet(logger, officers_owners, officers_owners_data.with_suffix(
    '.parquet'), compression_level=22)

#  REMOVE .FILES