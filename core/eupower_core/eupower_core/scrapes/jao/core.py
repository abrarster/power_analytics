import requests
import logging
import tenacity
import json
from typing import Optional, Dict, Any, Union
from datetime import datetime, date, time
from enum import Enum
from zoneinfo import ZoneInfo
from pathlib import Path

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Available data types from JAO publication tool.

    Each enum value is a tuple of (description, endpoint_url)
    """

    MAX_NET_POSITIONS = ("Max Net Positions", "maxNetPos")
    MAX_EXCHANGES = ("Max Exchanges (MaxBex)", "maxExchanges")
    INITIAL_COMPUTATION = ("Initial Computation (Virgin Domain)", "initialComputation")
    REMEDIAL_ACTION_PREVENTIVE = ("Remedial Action Preventive", "pra")
    REMEDIAL_ACTION_CURATIVE = ("Remedial Action Curative", "cra")
    VALIDATION_REDUCTIONS = ("Validation Reductions", "validationReductions")
    PRE_FINAL_COMPUTATION = (
        "Pre-Final Computation (Early Publication)",
        "preFinalComputation",
    )
    LONG_TERM_NOMINATION = ("Long Term Nomination", "ltn")
    FINAL_COMPUTATION = ("Final Computation", "finalComputation")
    LTA = ("LTA", "lta")
    FINAL_BILATERAL_EXCHANGE = (
        "Final Bilateral Exchange Restrictions",
        "bexRestrictions",
    )
    ALLOCATION_CONSTRAINTS = ("Allocation Constraints", "allocationConstraint")
    D2CF = ("D2CF", "d2CF")
    REFPROG = ("Refprog", "refprog")
    REFERENCE_NET_POSITION = ("Reference Net Position", "referenceNetPosition")
    ATC_CORE_EXTERNAL = ("ATCs on CORE external borders", "atc")
    SHADOW_AUCTION_ATC = ("Shadow Auction ATC", "shadowAuctionAtc")
    ACTIVE_FB_CONSTRAINTS = ("Active FB constraints", "shadowPrices")
    ACTIVE_LTA_CONSTRAINTS = ("Active LTA constraints", "activeLtaConstraint")
    CONGESTION_INCOME = ("Congestion Income (in €)", "congestionIncome")
    SCHEDULED_EXCHANGES = ("Scheduled Exchanges", "scheduledExchanges")
    NET_POSITION = ("Net Position", "netPos")
    INTRADAY_ATC = ("Intraday ATC", "intradayAtc")
    INTRADAY_NTC = ("Intraday NTC", "intradayNtc")
    PRICE_SPREAD = ("Price Spread", "priceSpread")
    SPANNING_DFP = ("Spanning / DFP", "spanningDefaultFBP")
    ALPHA_FACTOR = ("Alpha factor from MCP", "alphaFactor")

    def __init__(self, description: str, endpoint: str):
        self.description = description
        self.endpoint = endpoint


class JaoClient:
    """Base client for interacting with JAO (Joint Allocation Office) API.
    Returns raw response objects.
    """

    BASE_URL = "https://publicationtool.jao.eu/core/api"
    DATA_PATH = "data"
    MONITORING_PATH = "system/monitoring"
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
    TIMEZONE = ZoneInfo("Europe/Paris")

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        custom_logger: Optional[logging.Logger] = None,
    ):
        """Initialize JAO client.

        Args:
            custom_logger: Optional logger to use instead of default
            session: Optional requests.Session to use for making requests
        """
        self._logger = custom_logger or logger
        self.session = session or requests.Session()

    def log(self, level: int, msg: str, *args, **kwargs) -> None:
        """Log a message with the configured logger.

        Args:
            level: The log level (e.g., logging.INFO)
            msg: The message to log
            *args: Additional positional arguments for the logger
            **kwargs: Additional keyword arguments for the logger
        """
        self._logger.log(level, msg, *args, **kwargs)

    def _format_from_date(self, d: date) -> str:
        """Format start date to JAO API format.

        Converts Paris date to UTC midnight datetime string.
        For example, 2024-01-01 00:00 Paris becomes 2023-12-31T23:00:00.000Z UTC.

        Args:
            d: date in Europe/Paris timezone

        Returns:
            Formatted UTC datetime string for start of day
        """
        dt_paris = datetime.combine(d, time.min).replace(tzinfo=self.TIMEZONE)
        dt_utc = dt_paris.astimezone(ZoneInfo("UTC"))
        return dt_utc.strftime(self.DATE_FORMAT)

    def _format_to_date(self, d: date) -> str:
        """Format end date to JAO API format.

        Converts Paris date to UTC end-of-day datetime string.
        For example, 2024-01-01 23:00 Paris becomes 2024-01-01T22:00:00.000Z UTC.

        Args:
            d: date in Europe/Paris timezone

        Returns:
            Formatted UTC datetime string for end of day
        """
        dt_paris = datetime.combine(d, time(23, 0)).replace(tzinfo=self.TIMEZONE)
        dt_utc = dt_paris.astimezone(ZoneInfo("UTC"))
        return dt_utc.strftime(self.DATE_FORMAT)

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> requests.Response:
        """Make HTTP request to JAO API."""
        if not endpoint.startswith("system/"):
            endpoint = f"{self.DATA_PATH}/{endpoint.lstrip('/')}"

        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        for attempt in tenacity.Retrying(
            retry=_should_retry,
            stop=tenacity.stop_after_attempt(8),
            wait=tenacity.wait_exponential_jitter(initial=1, exp_base=2, jitter=30),
            before_sleep=lambda retry_state: self.log(
                logging.WARNING,
                f"Received 429, retrying in {retry_state.next_action.sleep} seconds...",
            ),
        ):
            with attempt:
                self.log(logging.DEBUG, f"Making request to {url} with params {params}")
                response = self.session.request(
                    method=method, url=url, params=params, **kwargs
                )
                response.raise_for_status()
                return response

    def get_data(
        self, data_type: DataType, from_date: date, to_date: date, **kwargs
    ) -> requests.Response:
        """Generic method to fetch data from any endpoint.

        Args:
            data_type: The type of data to fetch
            from_date: Start date in Europe/Paris timezone
            to_date: End date in Europe/Paris timezone
            **kwargs: Additional parameters to pass to the API

        Returns:
            Raw response from the API
        """
        params = {
            "FromUtc": self._format_from_date(from_date),
            "ToUtc": self._format_to_date(to_date),
            **kwargs,
        }

        return self._make_request(data_type.endpoint, params=params)

    def get_monitoring(self) -> requests.Response:
        """Get monitoring data from system endpoint."""
        return self._make_request(self.MONITORING_PATH)


class JaoJsonClient(JaoClient):
    """JSON client for interacting with JAO API.
    Returns parsed JSON responses.
    """

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Make HTTP request to JAO API and return JSON response."""
        response = super()._make_request(endpoint, method, params, **kwargs)
        return response.json()


class JaoFileClient(JaoClient):
    """File-saving client for interacting with JAO API.
    Saves responses to filesystem and returns the file path.
    """

    def _get_filename(self, params: Optional[Dict[str, Any]] = None) -> str:
        """Generate filename for the response.

        Args:
            params: Query parameters

        Returns:
            Formatted filename including timestamp and parameters
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if params:
            param_str = "_".join(f"{k}_{v}" for k, v in sorted(params.items()))
            return f"{timestamp}_{param_str}.json"

        return f"{timestamp}.json"

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        folder_path: Union[str, Path] = None,
        file_name: Optional[str] = None,
        **kwargs,
    ) -> Path:
        """Make HTTP request to JAO API and save response to file."""
        if folder_path is None:
            raise ValueError("folder_path must be provided")

        response = super()._make_request(endpoint, method, params, **kwargs)

        # Create full path: folder_path/endpoint/filename
        file_dir = Path(folder_path) / endpoint.replace("/", "_")
        file_dir.mkdir(parents=True, exist_ok=True)

        filename = file_name or self._get_filename(params)
        file_path = file_dir / filename

        self.log(logging.INFO, f"Writing response to {file_path}")
        with file_path.open("w") as f:
            json.dump(response.json(), f, indent=2)

        return file_path

    def get_data(
        self,
        data_type: DataType,
        from_date: date,
        to_date: date,
        folder_path: Union[str, Path],
        file_name: Optional[str] = None,
        **kwargs,
    ) -> Path:
        """Generic method to fetch data from any endpoint.

        Args:
            data_type: The type of data to fetch
            from_date: Start date in Europe/Paris timezone
            to_date: End date in Europe/Paris timezone
            folder_path: Directory where response will be saved
            **kwargs: Additional parameters to pass to the API

        Returns:
            Path to the saved response file
        """
        params = {
            "FromUtc": self._format_from_date(from_date),
            "ToUtc": self._format_to_date(to_date),
            **kwargs,
        }

        return self._make_request(
            data_type.endpoint,
            params=params,
            folder_path=folder_path,
            file_name=file_name,
        )

    def get_monitoring(self, folder_path: Union[str, Path]) -> Path:
        """Get monitoring data from system endpoint.

        Args:
            folder_path: Directory where response will be saved

        Returns:
            Path to the saved response file
        """
        return self._make_request(self.MONITORING_PATH, folder_path=folder_path)


def _should_retry(retry_state) -> bool:
    """Determine if request should be retried based on response.

    Retries only on HTTP 429 (Too Many Requests) errors.
    """
    return (
        isinstance(retry_state.outcome.exception(), requests.exceptions.HTTPError)
        and retry_state.outcome.exception().response.status_code == 429
    )
