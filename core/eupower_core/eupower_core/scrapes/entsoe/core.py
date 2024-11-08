from __future__ import annotations

import pandas as pd
import urllib3
import xml.etree.ElementTree as ET
import tenacity
import logging
from requests.exceptions import ConnectionError
from entsoe import EntsoeRawClient
from entsoe.exceptions import NoMatchingDataError
from entsoe.mappings import PSRTYPE_MAPPINGS, NEIGHBOURS, lookup_area
from pathlib import Path
from functools import wraps
from typing import Callable, Union, Optional
from . import xml_parsers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

__all__ = ["EntsoeScraper", "FileWritingEntsoeScraper", "EntsoeFileParser"]


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


def retry_on_http_400(cls):
    """Class decorator that adds retry logic to EntsoeScraper methods for HTTP 400 errors"""

    def should_retry_exception(exception):
        # Check if it's an HTTP 400 error
        return (
            isinstance(exception, urllib3.exceptions.HTTPError)
            and getattr(exception, "status", None) == 429
        ) or (isinstance(exception, ConnectionError))

    def method_wrapper(method: Callable) -> Callable:
        @wraps(method)
        @tenacity.retry(
            retry=tenacity.retry_if_exception(should_retry_exception),
            wait=tenacity.wait_exponential(multiplier=5, min=60, max=1800),
            stop=tenacity.stop_after_attempt(10),
            before_sleep=lambda retry_state: logger.warning(
                f"HTTP 400 error, retrying {method.__name__} in {retry_state.next_action.sleep} seconds..."
            ),
        )
        def wrapper(self, *args, **kwargs):
            return method(self, *args, **kwargs)

        return wrapper

    # Find and wrap all methods that aren't special methods (don't start with __)
    for attr_name, attr_value in cls.__dict__.items():
        if isinstance(attr_value, Callable) and not attr_name.startswith("__"):
            setattr(cls, attr_name, method_wrapper(attr_value))

    return cls


@retry_on_http_400
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
            if neighbour == "DE_AT_LU":
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

    def get_units(
        self, country_code: str, as_of_date: Optional[pd.Timestamp] = None
    ) -> dict[str, str]:
        if as_of_date is None:
            as_of_date = pd.Timestamp.today()

        params = {
            "documentType": "A95",
            "businessType": "B11",
            "Implementation_DateAndOrTime": as_of_date.strftime("%Y-%m-%d"),
            "BiddingZone_Domain": lookup_area(country_code).code,
        }
        response = self.client._base_request(params=params, start=as_of_date, end=as_of_date)
        response_xml = response.text
        return {f"A95_{country_code}": response_xml}


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
                    print(f"Writing {filepath}")
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(content)

            return results

        return wrapper

    # Replace the init
    cls.__init__ = new_init

    # Find and wrap all methods that return dict[str, str]
    for attr_name in dir(cls):
        attr_value = getattr(cls, attr_name)
        if (
            isinstance(attr_value, Callable)
            and getattr(attr_value, "__annotations__", {}).get("return")
            == "dict[str, str]"
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


@retry_on_http_400
@writes_to_files
class FileWritingEntsoeScraper(EntsoeScraper):
    """A subclass of EntsoeScraper that automatically writes XML responses to files.

    This class extends EntsoeScraper by automatically saving all XML responses
    to files in a specified directory. The filename is derived from the response
    metadata and includes the country code, fuel type (where applicable), and date range.
    """

    pass


class EntsoeFileParser:
    """Parser for ENTSOE XML files generated by FileWritingEntsoeScraper.

    This class reads XML files and parses them into pandas DataFrames based on their filename pattern:
    - A75_* : Generation by fuel type
    - A73_* : Generation by unit
    - A65_* : Load
    - A88_* : Cross-border flows
    """

    PARSERS = {
        "A75": xml_parsers.parse_entsoe_generation,
        "A73": xml_parsers.parse_entsoe_generation_by_unit,
        "A65": xml_parsers.parse_entsoe_load,
        "A88": xml_parsers.parse_entsoe_cross_border_flows,
        "A95": xml_parsers.parse_power_plants,
    }

    def __init__(self, input_dir: Union[str, Path]):
        self.input_dir = Path(input_dir)
        if not self.input_dir.exists():
            raise ValueError(f"Input directory {input_dir} does not exist")

    def parse_file(self, file_path: Path) -> pd.DataFrame:
        """Parse a single XML file based on its filename pattern."""
        doc_type = file_path.stem.split("_")[0]
        parser = self.PARSERS.get(doc_type)

        if not parser:
            raise ValueError(f"Unknown document type {doc_type} in file {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            xml_content = f.read()

        return parser(xml_content)

    def parse_files(self, pattern: str = "*.xml") -> pd.DataFrame:
        """Parse all matching XML files in the input directory and concatenate results.

        Parameters
        ----------
        pattern : str, optional
            Glob pattern to match files, defaults to "*.xml"

        Returns
        -------
        pd.DataFrame
            Concatenated DataFrame of all parsed files
        """
        dfs = []
        for file_path in self.input_dir.glob(pattern):
            try:
                df = self.parse_file(file_path)
                if df is not None and not df.empty:
                    dfs.append(df)
            except Exception as e:
                print(f"Error parsing {file_path}: {str(e)}")
                continue

        if not dfs:
            raise ValueError(
                f"No valid data found in {self.input_dir} matching {pattern}"
            )

        return pd.concat(dfs, ignore_index=True)

    def parse_by_type(self, doc_type: str) -> pd.DataFrame:
        """Parse all files of a specific document type.

        Parameters
        ----------
        doc_type : str
            Document type code (A75, A73, A65, or A88)

        Returns
        -------
        pd.DataFrame
            Concatenated DataFrame of all parsed files of the specified type
        """
        if doc_type not in self.PARSERS:
            raise ValueError(f"Unknown document type {doc_type}")

        return self.parse_files(pattern=f"{doc_type}_*.xml")
