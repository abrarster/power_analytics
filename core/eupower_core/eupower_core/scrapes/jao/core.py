import requests
from typing import Optional, Dict, Any, Literal, Tuple, Union
from datetime import datetime, date, time
import logging
from enum import Enum
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Available data types from JAO publication tool.

    Each enum value is a tuple of (description, endpoint_url)
    """

    MONITORING = ("Monitoring", "system/monitoring")
    MAX_NET_POSITIONS = ("Max Net Positions", "maxNetPositions")
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

    def __init__(self):
        """Initialize JAO client."""
        self.session = requests.Session()

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
            endpoint = f"{self.DATA_PATH}/{endpoint}"

        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        try:
            response = self.session.request(
                method=method, url=url, params=params, **kwargs
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to make JAO API request: {e}")
            raise

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
