from __future__ import annotations

import pandas as pd
import urllib3
import xml.etree.ElementTree as ET
import tenacity
import logging
from entsoe import EntsoeRawClient
from entsoe.exceptions import NoMatchingDataError
from entsoe.mappings import PSRTYPE_MAPPINGS, NEIGHBOURS
from pathlib import Path
from functools import wraps
from typing import Callable

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

__all__ = ["EntsoeScraper", "FileWritingEntsoeScraper"]


def chunk_dates(
    start: pd.Timestamp, end: pd.Timestamp, days_in_chunk: int
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    chunks = []
    current = start
    end = end + pd.Timedelta(days=1)

    while current < end:
        chunk_end = min(current + pd.Timedelta(days=days_in_chunk), end)
        chunks.append((current, chunk_end))
        current = chunk_end

    return chunks


class EntsoeScraper:

    def __init__(self, api_key: str):
        self.client: EntsoeRawClient = EntsoeRawClient(api_key=api_key)
        self.abs_start_date = None
        self.abs_end_date = None

    def set_dates(self, start: pd.Timestamp, end: pd.Timestamp) -> EntsoeScraper:
        self.abs_start_date = start
        self.abs_end_date = end
        return self

    def get_generation_by_fuel_type(
        self, country_code: str, fuel_type: str
    ) -> dict[str, str]:
        results = {}
        for start_date, end_date in chunk_dates(
            self.abs_start_date, self.abs_end_date, days_in_chunk=5
        ):
            response = self.client.query_generation(
                country_code=country_code,
                start=start_date,
                end=end_date + pd.Timedelta(days=1),
                psr_type=fuel_type,
            )
            results[
                f"A75_{country_code}_{fuel_type}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
            ] = response
        return results

    def get_generation_by_unit(
        self, country_code: str, fuel_type: str
    ) -> dict[str, str]:
        results = {}
        for start_date, end_date in chunk_dates(
            self.abs_start_date, self.abs_end_date, days_in_chunk=1
        ):
            response = self.client.query_generation_per_plant(
                country_code=country_code,
                start=start_date,
                end=end_date,
                psr_type=fuel_type,
            )
            results[
                f"A73_{country_code}_{fuel_type}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
            ] = response
        return results

    def get_load(self, country_code: str) -> dict[str, str]:
        results = {}
        for start_date, end_date in chunk_dates(
            self.abs_start_date, self.abs_end_date, days_in_chunk=5
        ):
            response = self.client.query_load(
                country_code=country_code,
                start=start_date,
                end=end_date + pd.Timedelta(days=1),
            )
            results[
                f"A65_{country_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
            ] = response
        return results

    def query_crossborder_flows(self, country_code: str) -> dict[str, str]:
        results = {}
        neighbours = NEIGHBOURS[country_code]
        for neighbour in neighbours:
            if neighbour == 'DE_AT_LU':
                continue
            for start_date, end_date in chunk_dates(
                self.abs_start_date, self.abs_end_date, days_in_chunk=5
            ):
                try:
                    response = self.client.query_crossborder_flows(
                        country_code_from=country_code,
                        country_code_to=neighbour,
                        start=start_date,
                        end=end_date + pd.Timedelta(days=1),
                    )
                    results[
                        f"A88_{country_code}_{neighbour}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
                    ] = response
                except NoMatchingDataError:
                    logger.warning(
                        f"No matching data for {country_code} to {neighbour} from {start_date} to {end_date}"
                    )
                    continue
        return results


def writes_to_files(cls):
    """Class decorator that adds file writing capability to EntsoeScraper methods"""
    original_init = cls.__init__
    original_doc = cls.__doc__ or ""

    def new_init(
        self, api_key: str, output_dir: str | Path | None = None, *args, **kwargs
    ):
        original_init(self, api_key, *args, **kwargs)
        self.output_dir = Path(output_dir) if output_dir else None

    def method_wrapper(method: Callable) -> Callable:
        @wraps(method)
        def wrapper(self, *args, **kwargs) -> dict[str, str]:
            results = method(self, *args, **kwargs)

            if self.output_dir is not None:
                self.output_dir.mkdir(parents=True, exist_ok=True)
                for filename, content in results.items():
                    filepath = self.output_dir / f"{filename}.xml"
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(content)

            return results

        return wrapper

    # Replace the init
    cls.__init__ = new_init

    # Find and wrap all methods that return dict[str, str]
    for attr_name, attr_value in cls.__dict__.items():
        if (
            isinstance(attr_value, Callable)
            and attr_value.__annotations__.get("return") == dict[str, str]
        ):
            setattr(cls, attr_name, method_wrapper(attr_value))

    # Update the docstring
    cls.__doc__ = f"""{original_doc}

    Enhanced with file writing capability.

    Parameters
    ----------
    api_key : str
        The ENTSO-E API key
    output_dir : str | Path | None, optional
        Directory where XML files will be saved. If None, no files will be written.

    Examples
    --------
    >>> scraper = FileWritingEntsoeScraper(api_key="your_key", output_dir="path/to/output")
    >>> scraper.set_dates(start, end)
    >>> results = scraper.get_load("DE")  # Saves XML files and returns results dict
    
    >>> # Or without file writing
    >>> scraper = FileWritingEntsoeScraper(api_key="your_key")
    >>> results = scraper.get_load("DE")  # Only returns results dict
    """

    return cls


@writes_to_files
class FileWritingEntsoeScraper(EntsoeScraper):
    """A subclass of EntsoeScraper that automatically writes XML responses to files.

    This class extends EntsoeScraper by automatically saving all XML responses
    to files in a specified directory. The filename is derived from the response
    metadata and includes the country code, fuel type (where applicable), and date range.
    """

    pass
