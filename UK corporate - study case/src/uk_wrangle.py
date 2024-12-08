import pathlib
import logging
import polars as pl
import pandas as pd
import re
from datetime import datetime

def get_year(a_date):
    if pd.isnull(a_date):  # Handle NaT or None
        return None
    if isinstance(a_date, (pd.Timestamp, datetime)):
        return a_date.year
    elif isinstance(a_date, str):
        row_date = datetime.strptime(a_date, '%Y-%m-%d')
        return row_date.year
    else:
        return None  # Fallback for unexpected cases


def days_between_dates(row, logger):
    """Calculate the number of days between two dates."""
    try:
        date1, date2 = row['incorporation_date'], row['date_of_cessation']

        # Convert to datetime if dates are strings
        if isinstance(date1, str):
            date1 = datetime.strptime(date1, "%Y-%m-%d")
        if isinstance(date2, str):
            date2 = datetime.strptime(date2, "%Y-%m-%d")

        # Return the absolute difference in days
        return abs((date2 - date1).days)
    except Exception as e:
        logger.error(f"Error processing row {row}: {e}")
        return None


def process_address(address, logger):
    """Extract city from the office address."""
    try:
        if not address or not isinstance(address, str):
            return None

        sub_match = re.search(r',\s*([\w\s]+),', address)
        if sub_match:
            city_candidate = sub_match.group(1).strip()
            if " " in city_candidate and any(char.isdigit() for char in city_candidate):
                final_match = re.search(r', [^,]+, ([^,]+)$', address)
                return final_match.group(1) if final_match else city_candidate
        return city_candidate
    except Exception as e:
        logger.error(f"Error processing address '{address}': {e}")
        return None


def process_country(address, logger):
    """Extract country from the office address."""
    try:
        if not address or not isinstance(address, str):
            return None

        final_match = re.search(r'^[^,]+, [^,]+, ([^,]+),', address)
        return final_match.group(1) if final_match else None
    except Exception as e:
        logger.error(f"Error processing country from address '{address}': {e}")
        return None