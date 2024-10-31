from __future__ import annotations

import pandas as pd
import requests
import json
import re
import logging
from datetime import date
from time import sleep


logger = logging.getLogger(__name__)
_zones = {"peninsula": "DEMANDAQH", "nacional": "DEMANDAQH"}


def get_balances(
    session: requests.Session, start_date: date, end_date: date, system="peninsula"
) -> pd.DataFrame:
    """
    Scrape balances from redelectrica demanda.ree.es API

    Parameters
    ----------
    session: requests.Session
    start_date: date
    end_date: date
    system: str
        Current valid parameters are peninsula, nacional

    Returns
    -------
    DataFrame
    """
    dates = [x.date() for x in pd.date_range(start_date, end_date, freq="D")]
    processed_results = []
    for dt in dates:
        for i in range(3):
            try:
                logger.info(f'Scraping balances for {dt.strftime("%Y-%m-%d")}')
                response = _get_single_day_balances(session, dt, system)
                processed_results.append(response)
                break
            except (
                requests.exceptions.HTTPError,
                requests.exceptions.ProxyError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as e:
                if i < 2:
                    logger.warning(
                        f'Error scraping balances for {dt.strftime("%Y-%m-%d")}, sleeping 60 seconds'
                    )
                    sleep(60)
                    logger.info(f'Retrying scrape for {dt.strftime("%Y-%m-%d")}')
                else:
                    raise
    return pd.concat(processed_results, ignore_index=True)


def _get_single_day_balances(
    session: requests.Session, for_day: date, system="peninsula"
) -> pd.DataFrame:
    url = _makeurl(for_day, system)
    response = session.get(url)
    response.raise_for_status()
    parsed_response = _response_to_dict(response)
    df_response = _response_to_frame(parsed_response)
    return (
        df_response
        .pipe(lambda x: x[~x.ts.str.contains('A')])  # Remove DST row
        .pipe(lambda x: x[~x.ts.str.contains('B')])  # Remove DST row
        .assign(ts=lambda x: pd.to_datetime(x.ts))
        .sort_values("ts")
        .reset_index(drop=True)
        .assign(requested_system=system, requested_date=pd.Timestamp(for_day))
    )


def _makeurl(for_day: date, system="peninsula"):
    zone = _zones[system]
    url = "https://demanda.ree.es/WSvisionaMoviles{2}Rest/resources/demandaGeneracion{2}?curva={0}&fecha={1}"
    return url.format(zone, for_day.strftime("%Y-%m-%d"), system.title())


def _response_to_dict(response: requests.Response) -> dict:
    return json.loads(re.sub("\);", "", re.sub("null\(", "", response.text)))


def _response_to_frame(parsed_response: dict) -> pd.DataFrame:
    return pd.DataFrame.from_records(parsed_response["valoresHorariosGeneracion"])
