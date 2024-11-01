from __future__ import annotations

import pandas as pd
import requests
import json
import logging
import traceback
import warnings
from time import sleep
from typing import Optional
from datetime import date, timedelta
from eupower_core.utils.exceptions import NoDataError
warnings.filterwarnings("ignore", category=FutureWarning)


logger = logging.getLogger(__name__)
__all__ = ["make_headers", "list_indicators", "get_indicator"]


def make_headers(api_key) -> dict[str, str]:
    headers = {}
    headers["Content-Type"] = "application/json"
    headers["Accept"] = "application/json; application/vnd.esios-api-v1+json"
    headers["Host"] = "api.esios.ree.es"
    headers["x-api-key"] = api_key
    headers["Authorization"] = f'Token token="{api_key}"'
    return headers


def list_indicators(session: requests.Session, headers: dict[str, str]) -> pd.DataFrame:
    url = "https://api.esios.ree.es/indicators"
    response = session.get(url, headers=headers, params={"locale": "en"})
    response.raise_for_status()
    indicators = json.loads(response.content)
    return pd.DataFrame.from_records(indicators["indicators"]).assign(
        description=lambda x: x.description.str.replace(r"<[^<>]*>", "", regex=True)
    )


def get_indicator(
    session: requests.Session,
    header: dict[str, str],
    indicator_id: int,
    start_date: date,
    end_date: date,
    time_agg: str = "avg",
    time_trunc: Optional[str] = None,
) -> pd.DataFrame:
    """

    Parameters
    ----------
    session: requests.Session
    header: dict[str, str]
    indicator_id: int
    start_date: date
    end_date: date
    time_agg: str
        Function for method to resample response timeseries. Valid
        parameters are 'avg' and 'sum'
    time_trunc: Optional str
        Function for period to resample response timeseries. Valid
        options are 'five_minutes', 'ten_minutes', 'fifteen_minutes',
        'hour', 'day', 'month', 'year'

    Returns
    -------
    DataFrame
    """
    url = f"https://api.esios.ree.es/indicators/{indicator_id}"
    params = {
        "locale": "en",
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": (end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
        "time_agg": time_agg,
    }
    if time_trunc is not None:
        params["time_trunc"] = time_trunc
    logger.info(f"Getting indicator: {indicator_id}")
    response = _do_request(session, url, header, params)
    response_dict = json.loads(response.content)["indicator"]
    metadata = {
        "indicator_id": response_dict["id"],
        "short_name": response_dict["short_name"],
        "composited": response_dict["composited"],
        "step_type": response_dict["step_type"],
        "disaggregated": response_dict["disaggregated"],
        "values_updated_at": pd.to_datetime(response_dict["values_updated_at"]),
    }
    if len(response_dict["values"]) == 0:
        raise NoDataError(f"No Data for indicator: {indicator_id}")

    data = (
        pd.DataFrame.from_records(response_dict["values"])
        .assign(
            # Using inefficient apply to handle utc offset switching during dst, otherwise pandas errors
            datetime=lambda x: pd.to_datetime(x.datetime).apply(
                lambda x: pd.Timestamp(x).tz_localize(None)
            ),
            datetime_utc=lambda x: pd.to_datetime(x.datetime_utc).dt.tz_localize(None),
        )
        .rename(columns={"datetime": "for_date_cet", "datetime_utc": "for_date"})
    )
    for k, v in metadata.items():
        data[k] = v
    return data[
        [
            "indicator_id",
            "short_name",
            "geo_id",
            "geo_name",
            "for_date",
            "for_date_cet",
            "value",
            "composited",
            "step_type",
            "disaggregated",
            "values_updated_at",
        ]
    ]


def _do_request(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    params: dict[str, str],
    n_retries: int = 2,
    sleep_time: int = 5,
) -> requests.Response:
    for i in range(n_retries):
        response = session.get(url, headers=headers, params=params)
        try:
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError:
            if response.status_code == 403:
                if i < n_retries - 1:
                    logger.warning(f"403 status code, waiting {sleep_time} seconds")
                    sleep(sleep_time)
                    continue
                else:
                    raise

    return response


def get_indicators(
    session: requests.Session,
    header: dict[str, str],
    indicators: list[int],
    start_date: date,
    end_date: date,
    time_agg: str = "avg",
    time_trunc: Optional[str] = None,
):
    results = []
    for indicator in indicators:
        try:
            results.append(
                get_indicator(
                    session,
                    header,
                    indicator,
                    start_date,
                    end_date,
                    time_agg,
                    time_trunc,
                )
            )
        except requests.exceptions.HTTPError:
            logger.warning(
                f"Could not get indicator: {indicator} \n {traceback.format_exc()}"
            )
            continue
        except NoDataError:
            logger.warning(f"No data for indicator: {indicator}")
    if len(results) == 0:
        raise Exception("No Indicators found")
    results = pd.concat(results, ignore_index=True)
    return results
