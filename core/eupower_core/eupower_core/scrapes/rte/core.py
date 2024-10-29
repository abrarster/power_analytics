from __future__ import annotations

import requests
import pandas as pd
import json
import io
import os
import tenacity
import logging
from zipfile import ZipFile
from datetime import date
from typing import Optional
from requests.exceptions import HTTPError
from tempfile import TemporaryDirectory

logger = logging.getLogger(__name__)

RTE_REGIONS = [
    "ARA",
    "BFC",
    "BRE",
    "CEN",
    "ACA",
    "NPP",
    "IDF",
    "NOR",
    "ALP",
    "LRM",
    "PAC",
    "PLO",
]
EXCHANGE_COUNTERPARTIES = {
    "DE": "10YCB-GERMANY--8",
    "GB": "10YGB----------A",
    "IFA": "10Y1001C--00098F",
    "IFA2": "17Y0000009369493",
    "BE": "10YBE----------2",
    "ES": "10YES-REE------0",
    "IT": "10YIT-GRTN-----B",
    "CH": "10YCH-SWISSGRIDZ",
}


def get_token(
    client_id: str,
    client_secret: str,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
):
    oauth_url = "https://digital.iservices.rte-france.com/token/oauth/"
    r = requests.post(
        oauth_url,
        auth=(client_id, client_secret),
        proxies=proxies,
        verify=False if cert is None else cert,
    )

    if r.ok:
        access_token = r.json()["access_token"]
        token_type = r.json()["token_type"]
    else:
        raise Exception

    return token_type, access_token


def query_generation_byunit(
    token_type: str,
    access_token: str,
    start_date: date,
    end_date: date,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
) -> pd.DataFrame:
    url = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_unit"
    response = _query_generation_endpoint(
        token_type, access_token, url, start_date, end_date, proxies, cert
    )
    response = json.loads(response.content)["actual_generations_per_unit"]
    response = pd.concat(
        [_parse_unit_generation_response(x) for x in response], ignore_index=True
    )
    return response.pipe(_strip_excess_hours, start_date=start_date, end_date=end_date)


def query_water_reservoir_balances(
    token_type: str,
    access_token: str,
    start_date: date,
    end_date: date,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
) -> pd.DataFrame:
    url = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/water_reserves"
    headers = {"Authorization": f"{token_type} {access_token}"}
    start_date_ = pd.to_datetime(start_date).tz_localize("CET").isoformat()
    end_date_ = pd.to_datetime(end_date).tz_localize("CET").isoformat()
    params = {"start_date": start_date_, "end_date": end_date_}
    response = requests.get(
        url,
        headers=headers,
        params=params,
        proxies=proxies,
        verify=False if cert is None else cert,
    )
    response.raise_for_status()
    response = json.loads(response.content)["water_reserves"]
    vals = response["values"]
    df = (
        pd.DataFrame.from_records(vals)
        .assign(
            start_date=lambda x: pd.to_datetime(x.start_date),
            end_date=lambda x: pd.to_datetime(x.end_date),
            updated_date=lambda x: pd.to_datetime(x.updated_date),
        )
        .sort_values("start_date")
    )
    return df


def _parse_unit_generation_response(unit_gen_data: dict):
    return pd.DataFrame.from_records(unit_gen_data["values"]).assign(
        unit_name=unit_gen_data["unit"]["name"],
        unit_code=unit_gen_data["unit"]["eic_code"],
        production_type=unit_gen_data["unit"]["production_type"],
        start_date=lambda x: _try_convert_tz(x.start_date),
        end_date=lambda x: _try_convert_tz(x.end_date),
        updated_date=lambda x: _try_convert_tz(x.updated_date),
    )[
        [
            "production_type",
            "unit_name",
            "unit_code",
            "start_date",
            "end_date",
            "updated_date",
            "value",
        ]
    ]


def _strip_excess_hours(frame: pd.DataFrame, start_date: date, end_date: date):
    start_time_adj = pd.to_datetime(start_date).tz_localize("CET").tz_convert("UTC")
    end_time_adj = (
        (pd.to_datetime(end_date) + pd.Timedelta(23, unit="h"))
        .tz_localize("CET")
        .tz_convert("UTC")
    )
    return frame[
        (frame.start_date >= start_time_adj) & (frame.start_date <= end_time_adj)
    ]


def query_generation_bytype(
    token_type: str,
    access_token: str,
    start_date: date,
    end_date: date,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
):
    url = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type"
    response = _query_generation_endpoint(
        token_type, access_token, url, start_date, end_date, proxies, cert
    )
    response = json.loads(response.content)["actual_generations_per_production_type"]
    response = pd.concat(
        [
            _parse_generation_bytype(x)
            for x in response
            if x["production_type"] != "TOTAL"
        ],
        ignore_index=True,
    )
    return response.pipe(_strip_excess_hours, start_date=start_date, end_date=end_date)


def query_generation_mix_15min(
    token_type: str,
    access_token: str,
    dt: date,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
):
    url = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/generation_mix_15min_time_scale"
    response = _query_generation_endpoint(
        token_type, access_token, url, dt, dt, proxies, cert
    )

    def _parse_response(r):
        return pd.DataFrame.from_records(r["values"]).assign(
            production_type=r["production_type"],
            production_subtype=r["production_subtype"],
        )

    response = json.loads(response.content)["generation_mix_15min_time_scale"]
    response = pd.concat([_parse_response(x) for x in response], ignore_index=True)
    response = (
        response.assign(start_date=lambda x: pd.to_datetime(x.start_date))
        .rename(columns={"value": "mw"})
        .set_index("start_date")
        .groupby(["production_type", "production_subtype"])
        .resample("H")
        .mw.mean()
        .reset_index()
    )
    return response


def _parse_generation_bytype(gen_data: dict):
    return pd.DataFrame.from_records(gen_data["values"]).assign(
        production_type=gen_data["production_type"],
        start_date=lambda x: _try_convert_tz(x.start_date),
        end_date=lambda x: _try_convert_tz(x.end_date),
        updated_date=lambda x: _try_convert_tz(x.updated_date),
    )[["production_type", "start_date", "end_date", "updated_date", "value"]]


def query_consolidated_consumption(
    token_type: str,
    access_token: str,
    start_date: date,
    end_date: date,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
):
    url = "https://digital.iservices.rte-france.com/open_api/consolidated_consumption/v1/consolidated_power_consumption"
    response = _query_generation_endpoint(
        token_type, access_token, url, start_date, end_date, proxies, cert
    )
    response = json.loads(response.content)["consolidated_power_consumption"]
    if len(response) == 0:
        df = pd.DataFrame(columns=["updated_date", "start_date", "end_date", "value"])
    else:
        df = (
            pd.DataFrame.from_records(response[0]["values"])
            .assign(
                start_date=lambda x: _try_convert_tz(x.start_date),
                end_date=lambda x: _try_convert_tz(x.end_date),
                updated_date=lambda x: _try_convert_tz(x.updated_date),
            )[["updated_date", "start_date", "end_date", "value"]]
            .sort_values("start_date")
        )
        df = df.pipe(_strip_excess_hours, start_date=start_date, end_date=end_date)
    return df


def query_realtime_consumption(
    token_type: str,
    access_token: str,
    start_date: date,
    end_date: date,
    data_type: str,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
):
    data_type_mapping = {
        "observed": "REALISED",
        "intraday_forecast": "ID",
    }
    data_type_val = data_type_mapping[data_type]
    url = "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term"
    headers = {"Authorization": f"{token_type} {access_token}"}
    params = {
        "type": data_type_val,
        "start_date": str(pd.to_datetime(start_date).tz_localize("CET")).replace(
            " ", "T"
        ),
        "end_date": str(
            (pd.to_datetime(end_date).tz_localize("CET") + pd.Timedelta(24, unit="H"))
        ).replace(" ", "T"),
    }
    response = requests.get(
        url,
        headers=headers,
        params=params,
        proxies=proxies,
        verify=False if cert is None else cert,
    )
    response.raise_for_status()
    response = json.loads(response.content)
    return (
        pd.DataFrame.from_records(response["short_term"][0]["values"])
        .assign(
            start_date=lambda x: pd.to_datetime(x.start_date),
            end_date=lambda x: pd.to_datetime(x.end_date),
            updated_date=lambda x: pd.to_datetime(x.updated_date),
            data_type=data_type,
        )[["data_type", "updated_date", "start_date", "end_date", "value"]]
        .sort_values("start_date")
    )


def query_physical_flows(
    token_type: str,
    access_token: str,
    start_date: date,
    end_date: date,
    counterparty: str,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
):
    eic_code = EXCHANGE_COUNTERPARTIES[counterparty]
    url = "https://digital.iservices.rte-france.com/open_api/physical_flow/v1/physical_flows"
    headers = {"Authorization": f"{token_type} {access_token}"}
    params = {
        "country_eic_code": eic_code,
        "start_date": str(pd.to_datetime(start_date).tz_localize("CET")).replace(
            " ", "T"
        ),
        "end_date": str(
            (pd.to_datetime(end_date).tz_localize("CET") + pd.Timedelta(24, unit="H"))
        ).replace(" ", "T"),
    }
    response = requests.get(
        url,
        headers=headers,
        params=params,
        proxies=proxies,
        verify=False if cert is None else cert,
    )
    response.raise_for_status()
    response = json.loads(response.content)["physical_flows"]
    response = (
        pd.concat([_parse_physical_flows(x) for x in response])[
            [
                "receiver_country_name",
                "sender_country_name",
                "start_date",
                "end_date",
                "updated_date",
                "value",
            ]
        ]
        .rename(columns={"value": "mw"})
        .assign(
            direction=lambda x: x.receiver_country_name.apply(
                lambda y: "imports" if y == "France" else "exports"
            )
        )
        .pivot(index="start_date", columns="direction", values="mw")
        .reset_index()
        .assign(net_imports=lambda x: x.imports - x.exports, counterparty=counterparty)[
            ["counterparty", "start_date", "imports", "exports", "net_imports"]
        ]
        .rename(columns={"start_date": "for_date"})
        .sort_values("for_date")
        .reset_index(drop=True)
    )
    return response


def _parse_physical_flows(phys_flows_response: dict):
    return pd.DataFrame.from_records(phys_flows_response["values"]).assign(
        receiver_country_name=phys_flows_response["receiver_country_name"],
        sender_country_name=phys_flows_response["sender_country_name"],
        start_date=lambda x: _try_convert_tz(x.start_date),
        end_date=lambda x: _try_convert_tz(x.end_date),
        updated_date=lambda x: _try_convert_tz(x.updated_date),
    )


def _try_convert_tz(x: pd.Series):
    try:
        y = pd.to_datetime(x)
        try:
            return y.dt.tz_convert("UTC")
        except AttributeError:
            return y.apply(lambda z: pd.to_datetime(z).tz_convert("UTC"))
    except ValueError:
        return pd.to_datetime(x, utc=True)


def _query_generation_endpoint(
    token_type: str,
    access_token: str,
    url: str,
    start_date: date,
    end_date: date,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
    stack_depth: int = 0,
):
    headers = {"Authorization": f"{token_type} {access_token}"}
    start_date_ = str(
        (
            pd.to_datetime(start_date) - pd.Timedelta(24 * stack_depth, unit="H")
        ).tz_localize("CET")
    ).replace(" ", "T")
    end_date_ = str(
        (pd.to_datetime(end_date) + pd.Timedelta(24, unit="H")).tz_localize("CET")
    ).replace(" ", "T")
    params = {"start_date": start_date_, "end_date": end_date_}
    response = requests.get(
        url,
        headers=headers,
        params=params,
        proxies=proxies,
        verify=False if cert is None else cert,
    )
    try:
        response.raise_for_status(),
    except HTTPError:
        if response.status_code == 400:
            error_code = _get_error_code(response.content)
            # Error handling for request period being too short. Typically occurs
            # when trying to retrieve a single day of data and the day falls on a
            # DST switching day.
            if "F05" in error_code and stack_depth < 6:
                return _query_generation_endpoint(
                    token_type,
                    access_token,
                    url,
                    start_date,
                    end_date,
                    proxies,
                    cert,
                    stack_depth + 1,
                )
            else:
                raise
        else:
            raise
    return response


def _get_error_code(response: bytes) -> str:
    return json.loads(response.decode())["error"]


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_random(60, 180),
    reraise=True,
)
def download_rte_generation_mix(
    target_date: date,
    region: Optional[str] = None,
    proxies: Optional[dict] = None,
    cert: Optional[str] = None,
) -> pd.DataFrame:
    url = f"https://eco2mix.rte-france.com/curves/eco2mixDl"
    params = {"date": target_date.strftime("%d/%m/%y")}
    if region is not None:
        assert region in RTE_REGIONS
        params["region"] = region
    logger.info(
        f"Downloading eco2mix data for {target_date.strftime('%d/%m/%y')} and {region or 'FR'}"
    )
    r = requests.get(
        url,
        params=params,
        stream=True,
        verify=False if cert is None else cert,
        proxies=proxies,
    )
    with TemporaryDirectory() as temp_dir:
        save_path = os.path.join(
            temp_dir,
            f'{target_date.strftime("%Y%m%d")}_{"FR" if region is None else region}.zip',
        )
        with open(save_path, "wb") as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)

        with ZipFile(save_path, "r") as f_in:
            parts = f_in.namelist()
            string = io.StringIO()
            string.write(f_in.read(parts[0]).decode("windows-1252"))
            string.seek(0)
            df = pd.read_csv(
                string, delimiter="\t", index_col=False, skipfooter=1, engine="python"
            )
    df = df[~pd.isna(df["Date"])]
    return df
