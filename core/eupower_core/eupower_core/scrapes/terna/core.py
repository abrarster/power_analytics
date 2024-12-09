import requests
import tenacity
from datetime import date, timedelta

__all__ = ["get_detailed_available_capacity", "get_token"]

def is_403_error(exception):
    """Check if the exception is a HTTP 403 error."""
    return (
        isinstance(exception, requests.HTTPError)
        and exception.response.status_code == 403
    )


def retry_on_unauthorized(
    wait_min: int = 10,
    wait_max: int = 120,
    max_attempts: int = 5,
    retry_on_403_only: bool = True,
):
    """
    Create a tenacity retry decorator with configurable parameters.

    Args:
        wait_min: Minimum wait time in seconds
        wait_multiplier: Multiplier for random wait time
        max_attempts: Maximum number of retry attempts
        retry_on_403_only: If True, only retry on HTTP 403 errors. If False, retry on all HTTPErrors
    """
    retry_condition = (
        tenacity.retry_if_exception(is_403_error)
        if retry_on_403_only
        else tenacity.retry_if_exception_type(requests.HTTPError)
    )

    return tenacity.retry(
        retry=retry_condition,
        wait=tenacity.wait_random(min=wait_min, max=wait_max),
        stop=tenacity.stop_after_attempt(max_attempts),
        before_sleep=lambda retry_state: print(
            f"Retrying after {retry_state.outcome.exception()}"
        ),
        reraise=True,
    )


def get_token(session: requests.Session, client_id: str, client_secret: str):
    url = "https://api.terna.it/public-api/access-token"
    params = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    response = session.post(url, headers=headers, data=params)
    response.raise_for_status()
    return response.json()["access_token"]


def _get_data(session: requests.Session, endpoint: str, token: str, params: dict):
    root_url = f"https://api.terna.it/{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    response = session.get(root_url, headers=headers, params=params)
    response.raise_for_status()
    return response


@retry_on_unauthorized()
def get_detailed_available_capacity(
    session: requests.Session, token: str, start_date: date, end_date: date
):
    params = {
        "dateFrom": start_date.strftime("%d/%m/%Y"),
        "dateTo": (end_date + timedelta(days=0)).strftime("%d/%m/%Y"),
    }
    return _get_data(session, "adequacy/v1.0/detail-available-capacity", token, params)
