import requests
from typing import Optional, Dict, Any, Literal, Tuple
from datetime import datetime, date
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Available data types from JAO publication tool.

    Each enum value is a tuple of (description, endpoint_url)
    """

    MONITORING = ("Monitoring", "monitoring")
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


class JAOClient:
    """Client for interacting with JAO (Joint Allocation Office) API."""

    BASE_URL = "https://publicationtool.jao.eu/core/api/data"

    def __init__(self):
        """Initialize JAO client."""
        self.session = requests.Session()

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Make HTTP request to JAO API.

        Args:
            endpoint: API endpoint to call
            method: HTTP method to use
            params: Query parameters to include
            **kwargs: Additional arguments to pass to requests

        Returns:
            JSON response from the API

        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        try:
            response = self.session.request(
                method=method, url=url, params=params, **kwargs
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to make JAO API request: {e}")
            raise
