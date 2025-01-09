"""
Module for Data Processing and Extraction.

This module provides helper functions to process and extract information
from data related to dates and addresses. It includes functionality to
extract years, calculate days between dates, and parse city or country
information from address strings.

This module is designed for data preprocessing in ETL pipelines or similar
data analysis workflows where structured date and address handling is required.
"""


from typing import Optional, Union
import pandas as pd
from re import search
from datetime import datetime
import logging


def get_year(a_date: Union[pd.Timestamp, datetime, str, None]
             ) -> Optional[int]:
    """
    Extract the year from a given date.

    Parameters
    ----------
    a_date : Union[pd.Timestamp, datetime, str, None]
        The date from which to extract the year. Can be a pandas Timestamp,
        a Python datetime object, a string in the format '%Y-%m-%d', or None.

    Returns
    -------
    Optional[int]
        The year as an integer if extraction is successful, otherwise None.
    """
    if pd.isnull(a_date):  # Handle NaT or None
        return None
    if isinstance(a_date, (pd.Timestamp, datetime)):
        return a_date.year
    elif isinstance(a_date, str):
        row_date = datetime.strptime(a_date, '%Y-%m-%d')
        return row_date.year
    else:
        return None  # Fallback for unexpected cases


def days_between_dates(row: pd.Series,
                       logger: logging.Logger
                       ) -> Optional[int]:
    """
    Calculate the number of days between two dates in a given row.

    Parameters
    ----------
    row : pd.Series
        A row of data containing 'incorporation_date' and 'date_of_cessation'.
    logger : logging.Logger
        A logger instance to log errors.

    Returns
    -------
    Optional[int]
        The absolute number of days between the two dates, or None if an error occurs.

    Notes
    -----
    - Dates can be in string format ('%Y-%m-%d') or as datetime objects.
    - Handles invalid date formats or missing data gracefully by returning None.
    """
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


def process_address(address: Optional[str],
                    logger: logging.Logger
                    ) -> Optional[str]:
    """
    Extract the city from an office address.

    Parameters
    ----------
    address : Optional[str]
        A string representing the office address.
    logger : logging.Logger
        A logger instance to log errors.

    Returns
    -------
    Optional[str]
        The extracted city if found, otherwise None.

    Notes
    -----
    - The function uses regex to extract a candidate city from the address.
    - Handles cases where the address has numeric characters within city-like strings.
    - Logs an error if processing fails or the input is invalid.
    """
    try:
        if not address or not isinstance(address, str):
            return None

        sub_match = search(r',\s*([\w\s]+),', address)
        if sub_match:
            city_candidate = sub_match.group(1).strip()
            if " " in city_candidate and any(char.isdigit() for char in city_candidate):
                final_match = search(r', [^,]+, ([^,]+)$', address)
                return final_match.group(1) if final_match else city_candidate
        return city_candidate
    except Exception as e:
        logger.error(f"Error processing address '{address}': {e}")
        return None


def process_country(address: Optional[str],
                    logger: logging.Logger
                    ) -> Optional[str]:
    """
    Extract the country from an office address.

    Parameters
    ----------
    address : Optional[str]
        A string representing the office address.
    logger : logging.Logger
        A logger instance to log errors.

    Returns
    -------
    Optional[str]
        The extracted country if found, otherwise None.

    Notes
    -----
    - Uses regex to identify the country in the address format.
    - Returns None for invalid or missing input.
    - Logs errors for exceptions during processing.
    """
    try:
        if not address or not isinstance(address, str):
            return None

        final_match = search(r'^[^,]+, [^,]+, ([^,]+),', address)
        return final_match.group(1) if final_match else None
    except Exception as e:
        logger.error(f"Error processing country from address '{address}': {e}")
        return None
