from __future__ import annotations

import requests
import io
import ast
import pandas as pd
from datetime import datetime, date

__all__ = [
    "download_capacities",
    "download_power_balance",
    "download_water_balance",
    "download_hydro_production",
]
_root = "http://datahub.ren.pt/service/download/csv/{endpoint}"


def download_water_balance(session: requests.Session, for_day: date) -> str:
    """
    Download daily water balance from REN as semicolon separated string
    """
    return _do_download(session, for_day, 1535)


def download_power_balance(session: requests.Session, for_day: date) -> str:
    """
    Download 15 minute power balances from REN
    """
    return _do_download(session, for_day, 1354)


def download_capacities(session: requests.Session, for_day: date) -> str:
    return _do_download(session, for_day, 2768)


def download_hydro_production(target_date: date) -> pd.DataFrame:
    """
    Fetch and process hydro production data from REN for a specific date

    Args:
        target_date (date): The date to fetch data for

    Returns:
        pd.DataFrame: DataFrame with columns for each hydro production type and timestamps as index
    """
    # Convert date to required timestamp format
    timestamp = int(
        (
            datetime(target_date.year, target_date.month, target_date.day)
            - datetime(1, 1, 1)
        ).total_seconds()
        * 10000000
    )

    # Setup request
    url = "https://datahub.ren.pt/service/Electricity/HydricProduction/1386"
    params = {"culture": "en-GB", "dayToSearchString": str(timestamp)}
    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Origin": "https://datahub.ren.pt",
        "Referer": "https://datahub.ren.pt/en/electricity/daily-balance/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    # Make request
    response = requests.post(url, params=params, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: HTTP {response.status_code}")

    # Parse response
    data = response.json()
    if "series" not in data:
        raise Exception("Unexpected response format")

    # Convert to DataFrame
    df = pd.DataFrame(data["series"])

    # Transform the data
    def transform_hydro_data(df):
        # Convert string representations of lists to actual lists if needed
        if isinstance(df["data"].iloc[0], str):
            df["data"] = df["data"].apply(ast.literal_eval)

        # Create a dictionary to store the series
        series_dict = {}

        # Extract data for each type of hydro production
        for _, row in df.iterrows():
            name = row["name"] if pd.notna(row["name"]) else "Pumping"
            series_dict[name] = row["data"]

        # Create a DataFrame with all series
        result_df = pd.DataFrame(series_dict)

        # Add timestamp index (assuming 15-minute intervals for a day)
        timestamps = pd.date_range(
            start=target_date, periods=len(result_df), freq="15T"
        )
        result_df.index = timestamps

        # Convert pumping values to positive
        if "Pumping" in result_df.columns:
            result_df["Pumping"] = result_df["Pumping"].abs()

        # Add total generation
        generation_columns = [col for col in result_df.columns if col != "Pumping"]
        result_df["Total Generation"] = result_df[generation_columns].sum(axis=1)
        return (
            result_df.rename_axis("for_date")
            .reset_index()
            .rename(
                columns={
                    "Pumping": "pumped_storage",
                    "Small Hydro": "small_hydro",
                    "Run-of-River": "run_of_river",
                    "Reservoirs": "reservoir",
                    "Total Generation": "total_hydro",
                }
            )
        )

    return transform_hydro_data(df)


def _do_download(session: requests.Session, for_day: date, endpoint_id: int) -> str:
    url = _root.format(endpoint=endpoint_id)
    date_str = for_day.strftime("%Y-%m-%d")
    payload = {
        "startDateString": date_str,
        "endDateString": date_str,
        "culture": "en-GB",
    }
    response = session.get(url=url, params=payload)
    response.raise_for_status()
    processed_response = io.StringIO(response.content.decode("utf-8")).read()
    return processed_response
