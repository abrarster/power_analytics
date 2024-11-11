from __future__ import annotations

import requests
import io
from datetime import date

__all__ = ["download_capacities", "download_power_balance", "download_water_balance"]
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
