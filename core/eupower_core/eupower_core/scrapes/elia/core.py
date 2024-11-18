from __future__ import annotations

import json
import requests
import urllib3
import pandas as pd
from enum import Enum
from datetime import date, timedelta
from typing import Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ROOT = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/{series_id}/exports/json"
REMIT_ROOT = (
    "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/{series_id}/records"
)


class EliaTs(Enum):
    TOTAL_LOAD = "ods001"
    GRID_LOAD = "ods003"
    RT_LOAD = "ods002"
    DA_GEN_BYFUEL_OLD = "ods034"
    DA_GEN_BYFUEL = "ods176"
    RT_GEN_BYFUEL_OLD = "ods033"
    RT_GEN_BYFUEL = "ods177"
    AVAILABILITY_BYFUEL = "ods037"
    AVAILABILITY_BYUNIT = "ods038"
    INSTALLED_CAP_BYFUEL = "ods035"
    WIND_GENERATION_HIST = "ods031"
    WIND_GENERATION_RT = "ods086"
    SOLAR_GENERATION_HIST = "ods032"
    SOLAR_GENERATION_RT = "ods087"
    SYSTEM_IMBALANCE = "ods045"
    ITC_DA_COMEX = "ods015"
    ITC_TOTAL_COMEX = "ods016"
    ITC_PHYS_FLOW = "ods026"
    GENERATION_UNITS = "ods179"

    def exec_query(
        self, start: date, end: date, cert: Optional[str] = None
    ) -> requests.Response:
        if self.name == "GENERATION_UNITS":
            return get_units(start)
        else:
            return execute_ts_query(self.value, start, end, cert)


class EliaRemits(Enum):
    PLANNED_OUTAGES = "ods180"
    FORCED_OUTAGES = "ods181"

    def exec_query(
        self, remit_start_date: date, remit_end_date: date, cert: Optional[str] = None
    ) -> requests.Response:
        response = execute_remit_query(
            self.value, remit_start_date, remit_end_date, cert
        )
        return response


def get_units(as_of_date: date) -> requests.Response:
    session = requests.Session()
    url = REMIT_ROOT.format(series_id="ods179")
    where_clause = []
    if as_of_date:
        where_clause.append(
            "date>=date'{}'".format(as_of_date.strftime("%Y-%m-%d"))
        )
    if as_of_date:
        where_clause.append(
            "date<date'{}'".format(
                (as_of_date + timedelta(days=1)).strftime("%Y-%m-%d")
            )
        )
    if len(where_clause) > 0:
        where_clause = " and ".join(where_clause)
    else:
        where_clause = None
    params = {"limit": -1, "timezone": "UTC"}
    if where_clause:
        params["where"] = where_clause
    response = session.get(url, params=params)
    response.raise_for_status()
    return response


def execute_ts_query(
    series_id: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
    cert: Optional[str] = None,
    **params,
) -> requests.Response:
    session = requests.Session()
    session.verify = False if not cert else cert
    url = ROOT.format(series_id=series_id)
    where_clause = []
    if start:
        where_clause.append("datetime>=date'{}'".format(start.strftime("%Y-%m-%d")))
    if end:
        where_clause.append("datetime<=date'{}'".format(end.strftime("%Y-%m-%d")))
    if len(where_clause) > 0:
        where_clause = " and ".join(where_clause)
    else:
        where_clause = None
    params = {"limit": -1, "orderby": "datetime", "timezone": "UTC"}
    if where_clause:
        params["where"] = where_clause
    response = session.get(url, params=params)
    response.raise_for_status()
    return response


def execute_remit_query(
    series_id: str,
    remit_start_date: date,
    remit_end_date: date,
    cert: Optional[str] = None,
) -> requests.Response:
    session = requests.Session()
    session.verify = False if not cert else cert
    url = REMIT_ROOT.format(series_id=series_id)
    where_clause = []
    where_clause.append(
        "lastupdated>=date'{}'".format(remit_start_date.strftime("%Y-%m-%d"))
    )
    where_clause.append(
        "lastupdated<date'{}'".format(
            (remit_end_date + timedelta(days=1)).strftime("%Y-%m-%d")
        )
    )
    params = {
        "limit": -1,
        "orderby": "lastupdated",
        "timezone": "UTC",
        "where": where_clause,
    }
    response = session.get(url, params=params)
    return response
