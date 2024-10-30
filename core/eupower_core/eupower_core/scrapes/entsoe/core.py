from __future__ import annotations

import pandas as pd
import urllib3
import xml.etree.ElementTree as ET
import tenacity
from entsoe import EntsoeRawClient
from entsoe.mappings import PSRTYPE_MAPPINGS, CountryCode, NEIGHBOURS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    ) -> dict[str, pd.DataFrame]:
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
    ) -> dict[str, pd.DataFrame]:
        results = {}
        for start_date, end_date in chunk_dates(
            self.abs_start_date, self.abs_end_date, days_in_chunk=1
        ):
            response = self.client.query_generation_per_plant(
                country_code=country_code,
                start=start_date,
                end=end_date + pd.Timedelta(days=1),
                psr_type=fuel_type,
            )
            results[
                f"A73_{country_code}_{fuel_type}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
            ] = response
        return results

    def get_load(self, country_code: str) -> dict[str, pd.DataFrame]:
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

    def query_crossborder_flows(self, country_code: str) -> dict[str, pd.DataFrame]:
        results = {}
        neighbours = NEIGHBOURS[country_code]
        for neighbour in neighbours:
            for start_date, end_date in chunk_dates(
                self.abs_start_date, self.abs_end_date, days_in_chunk=5
            ):
                response = self.client.query_crossborder_flows(
                    country_code_from=country_code,
                    country_code_to=neighbour,
                    start=start_date,
                    end=end_date + pd.Timedelta(days=1),
                )
                results[
                    f"A88_{country_code}_{neighbour}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
                ] = response
        return results
