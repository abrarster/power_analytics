import io
import requests
import zipfile
import pandas as pd
from datetime import date
from . import endpoints

_root = "https://www.omie.es/en/file-download"


def download_from_omie_fs(
    session: requests.Session, endpoint: str, file_date: date
) -> str | zipfile.ZipFile:
    """
    Download omie market data

    Parameters
    ----------
    session: requests.Session
    endpoint: str
        Select endpoint from eupower_core.third_party.omie.endpoints. Options are:
        Monthly files (published 3 months in arrears)
            CAB: Day ahead bid header files
            DET: Day ahead bid detail files
            UNIT_CURVES: Day ahead bid offer curves with units
            PDBC: Base matching schedule
            PDBF: Base operational schedule
        Daily files (published at 1:15pm CET)
            DA_CLEAR: cleared marginal prices
            PDBC_STOTA: disaggregated power after day ahead market

    file_date: date
        As of date for file

    Returns
    -------
    Monthly data returns zip files, daily data returns string
    """
    endpoint_schema = endpoints._endpoints[endpoint]
    if endpoint_schema["frequency"] == "monthly":
        response = _download_monthly_zipfile(session, endpoint, file_date)
    elif endpoint_schema["frequency"] == "daily":
        response = _download_daily_textfile(session, endpoint, file_date)
    else:
        raise NotImplementedError
    return response


def _download_monthly_zipfile(
    session: requests.Session, endpoint: str, file_date: date
) -> zipfile.ZipFile:
    """
    Download zipfile from OMIE containing monthly data.

    Parameters
    ----------
    session: requests.Session
    endpoint: str
        OMIE endpoint. Valid values are:
            'cab': Headers of bids for Day Ahead market
            'det': Day ahead market bids detail
            'indisp': Unavailability of spanish bid units
    file_date: date

    Returns
    -------
    ZipFile
    """
    endpoint_schema = endpoints._endpoints[endpoint]
    if endpoint_schema["raw_data_type"] != "zipfile":
        raise TypeError
    if endpoint_schema["frequency"] != "monthly":
        raise TypeError

    year = file_date.year
    month = file_date.month
    params = {
        "parents[0]": endpoint.lower(),
        "filename": f"{endpoint.lower()}_{year}{month:02d}.zip",
    }
    response = session.get(_root, params=params, stream=True)
    response.raise_for_status()
    return zipfile.ZipFile(io.BytesIO(response.content))


def _download_daily_textfile(
    session: requests.Session, endpoint: str, file_date: date
) -> str:
    endpoint_schema = endpoints._endpoints[endpoint]
    if endpoint_schema["raw_data_type"] != "text":
        raise TypeError
    if endpoint_schema["frequency"] != "daily":
        raise TypeError
    params = {
        "parents[0]": endpoint.lower(),
        "filename": f"{endpoint.lower()}_{file_date.year}{file_date.month:02d}{file_date.day:02d}.1",
    }
    response = session.get(_root, params=params)
    response.raise_for_status()
    try:
        return response.content.decode("utf-8")
    except UnicodeDecodeError:
        return response.content.decode("latin-1")


def parse_file(file_object: io.StringIO | io.BytesIO, endpoint: str) -> pd.DataFrame:
    """
    Process raw downloaded omie files into clean format

    Parameters
    ----------
    file_object: io.StringIO | io.BytesIO
        In memory loaded raw file downloaded from Omie
    endpoint: str
        OMIE endpoint available at eupower_core.third_party.omie.endpoints

    Returns
    -------
    pd.DataFrame
    """
    endpoint_schema = endpoints._endpoints[endpoint]
    text_format = endpoint_schema["text_format"]
    if text_format == "fwf":
        parsed_file = _read_fwf(file_object, endpoint_schema["filespec"])
    elif text_format == "csv":
        parsed_file = _read_csv(file_object, endpoint_schema["filespec"])
    else:
        raise NotImplementedError
    return parsed_file


def _read_fwf(file_object, filespec) -> pd.DataFrame:
    return pd.read_fwf(
        file_object,
        colspecs=filespec["colspecs"],
        names=filespec["colnames"],
        encoding="latin-1",
    )


def _read_csv(file_object, filespec) -> pd.DataFrame:
    return pd.read_csv(
        file_object,
        sep=";",
        skiprows=filespec["skiplines"],
        skipfooter=filespec["skipfooter"],
        header=None,
        names=filespec["colnames"],
        encoding="latin-1",
    )
