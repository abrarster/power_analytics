import pandas as pd
import requests
import tenacity
import logging
from time import sleep
from datetime import date, timedelta
from math import ceil
from eupower_core.utils.exceptions import NoDataError
from . import parameters as params

logger = logging.getLogger(__name__)
BASE_URL = "https://api.ned.nl/v1/utilizations"


@tenacity.retry(
    wait=tenacity.wait_fixed(30) + tenacity.wait_random(0, 60),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(
        (
            requests.exceptions.ProxyError,
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
        )
    ),
    reraise=True,
)
def download_from_ned(
    session: requests.Session,
    api_key: str,
    start_date: date,
    end_date: date,
    point: params.Point,
    classification: params.Classification,
    granularity: params.Granularity,
    time_zone: params.TimeZone,
    activity: params.Activity,
    data_type: params.DataType,
) -> requests.Response:
    adj_end_date = end_date + timedelta(days=1)
    assert adj_end_date <= start_date + pd.to_timedelta(7, unit="D")

    headers = {
        "X-AUTH-TOKEN": api_key,
        "accept": "application/ld+json",
    }

    params = {
        "point": point.value,
        "type": data_type.value,
        "granularity": granularity.value,
        "granularitytimezone": time_zone.value,
        "classification": classification.value,
        "activity": activity.value,
        "validfrom[strictly_before]": end_date.strftime("%Y-%m-%d"),
        "validfrom[after]": start_date.strftime("%Y-%m-%d"),
    }

    while True:
        response = session.get(
            BASE_URL, headers=headers, params=params, allow_redirects=False
        )
        if response.status_code == 429:
            logger.warning(f"Rate limit exceeded, waiting 120 seconds")
            sleep(120)
            logger.info(f"Continuing")
            continue
        else:
            break

    return response


def parse_ned_response(r: requests.Response) -> pd.DataFrame:
    response = r.json()
    if len(response["hydra:member"]) == 0:
        raise NoDataError
    df_raw = pd.DataFrame.from_records(response["hydra:member"])

    df = df_raw.assign(
        granularity=lambda x: _clean_column(
            x["granularity"], "/v1/granularities/", params.Granularity
        ),
        timezone=lambda x: _clean_column(
            x["granularitytimezone"], "/v1/granularity_time_zones/", params.TimeZone
        ),
        activity=lambda x: _clean_column(
            x["activity"], "/v1/activities/", params.Activity
        ),
        classification=lambda x: _clean_column(
            x["classification"], "/v1/classifications/", params.Classification
        ),
        point=lambda x: _clean_column(x["point"], "/v1/points/", params.Point),
        type=lambda x: _clean_column(x["type"], "/v1/types/", params.DataType),
    ).drop(columns="granularitytimezone")

    return df


def _clean_column(x: pd.Series, prefix: str, e) -> dict:
    dct = {i.value: i.name for i in e}
    x = x.str.replace(prefix, "").astype(int).replace(dct)
    return x


def scrape(
    session: requests.Session,
    api_key: str,
    start_date: date,
    end_date: date,
    point=params.Point.NETHERLANDS,
    classification=params.Classification.CURRENT,
    granularity=params.Granularity.HOUR,
    time_zone=params.TimeZone.UTC,
    activity=params.Activity.GENERATION,
    data_type=params.DataType.ALL,
):
    day = pd.to_timedelta(1, unit="D")
    n_downloads = (end_date - start_date) / (6 * day)

    res = []

    for i in range(ceil(n_downloads)):
        sub_start = start_date + i * 6 * day
        sub_end = min(end_date, sub_start + 7 * day)

        print(sub_start)

        r = download_from_ned(
            session,
            api_key,
            start_date=sub_start,
            end_date=sub_end,
            point=point,
            classification=classification,
            granularity=granularity,
            time_zone=time_zone,
            activity=activity,
            data_type=data_type,
        )

        res.append(parse_ned_response(r=r))

    return pd.concat(res)
