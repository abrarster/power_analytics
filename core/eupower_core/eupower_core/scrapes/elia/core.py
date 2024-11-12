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
    INSTALLED_CAP_BYUNIT_OLD = "ods036"
    INSTALLED_CAP_BYUNIT = "ods179"
    WIND_GENERATION_HIST = "ods031"
    WIND_GENERATION_RT = "ods086"
    SOLAR_GENERATION_HIST = "ods032"
    SOLAR_GENERATION_RT = "ods087"
    SYSTEM_IMBALANCE = "ods045"
    ITC_DA_COMEX = "ods015"
    ITC_TOTAL_COMEX = "ods016"
    ITC_PHYS_FLOW = "ods026"

    def exec_query(
        self, start: date, end: date, cert: Optional[str] = None
    ) -> requests.Response:
        return execute_ts_query(self.value, start, end, cert)


class EliaRemits(Enum):
    PLANNED_OUTAGES = "ods040"
    FORCED_OUTAGES = "ods039"

    def exec_query(
        self,
        remit_start_date: date,
        remit_end_date: date,
        cert: Optional[str] = None
    ) -> pd.DataFrame:
        response = execute_remit_query(self.value, remit_start_date, remit_end_date, cert)
        return (
            pd.DataFrame.from_records(response)
            .assign(
                startdatetime=lambda x: pd.to_datetime(x.startdatetime),
                enddatetime=lambda x: pd.to_datetime(x.enddatetime),
                startoutagetstime=lambda x: pd.to_datetime(x.startoutagetstime),
                endoutagetstime=lambda x: pd.to_datetime(x.endoutagetstime),
                lastupdated=lambda x: pd.to_datetime(x.lastupdated),
            )
            .sort_values(by="lastupdated", ascending=True)
        )


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
    cert: Optional[str] = None
):
    session = requests.Session()
    session.verify = False if not cert else cert
    url = ROOT.format(series_id=series_id)
    where_clause = []
    where_clause.append("lastupdated>=date'{}'".format(remit_start_date.strftime("%Y-%m-%d")))
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
    response.raise_for_status()
    return json.loads(response.content)
