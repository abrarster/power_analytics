import requests
import json
import dlt
import pandas as pd
import tenacity
import logging
from typing import Any, Optional
from pathlib import Path
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Add this at the module level, before create_jao_pipeline
_jao_sources = {}


def _format_from_date(d: str) -> str:
    """Format start date to JAO API format.

    Converts Paris date to UTC midnight datetime string.
    For example, 2024-01-01 00:00 Paris becomes 2023-12-31T23:00:00.000Z UTC.

    Args:
        d: date in Europe/Paris timezone

    Returns:
        Formatted UTC datetime string for start of day
    """
    dt_paris = datetime.combine(pd.to_datetime(d).date(), time.min).replace(
        tzinfo=ZoneInfo("Europe/Paris")
    )
    dt_utc = dt_paris.astimezone(ZoneInfo("UTC"))
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _format_to_date(d: str) -> str:
    dt_paris = datetime.combine(pd.to_datetime(d).date(), time(23, 0)).replace(
        tzinfo=ZoneInfo("Europe/Paris")
    )
    dt_utc = dt_paris.astimezone(ZoneInfo("UTC")) + timedelta(hours=1)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def save_json_to_filesystem(
    data: dict, resource_name: str, as_of_date: str, root_dir: Path = Path("data")
) -> str:
    """Save raw JSON response to filesystem.

    Args:
        data: The data to save
        resource_name: Name of the resource (used in path)
        as_of_date: Date of the data
        root_dir: Root directory for file storage (default: ./data)

    Returns:
        str: Path to saved file
    """
    dt = pd.to_datetime(as_of_date)
    output_dir = (
        root_dir
        / "jao"
        / str(dt.year)
        / f"{dt.month:02d}"
        / f"{dt.day:02d}"
        / resource_name.lower()
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{resource_name.lower()}_{dt.strftime('%Y%m%d')}.json"
    output_path = output_dir / filename

    with open(output_path, "w") as f:
        json.dump(data, f)

    return str(output_path)


def create_jao_pipeline(
    resource_name: str, endpoint_path: str, additional_params: Optional[dict] = None
):
    """Factory function to create a source with a single resource.

    Args:
        resource_name: Name of the resource
        endpoint_path: API endpoint path
    """

    @dlt.resource(name=resource_name, write_disposition="merge", primary_key="id")
    def resource(
        as_of_date: str,
        root_path: Path,
        resource_logger: Optional[logging.Logger] = None,
    ):
        nonlocal additional_params
        base_url = "https://publicationtool.jao.eu/core/api"
        params = {
            "fromUTC": _format_from_date(as_of_date),
            "toUTC": _format_to_date(as_of_date),
        }
        if additional_params is not None:
            params.update(additional_params)

        endpoint = f"{base_url}/{endpoint_path}"
        if resource_logger is None:
            instance_logger = logger
        else:
            instance_logger = resource_logger
        instance_logger.debug(f"Fetching {resource_name} data for {as_of_date}")
        for attempt in tenacity.Retrying(
            retry=_should_retry,
            stop=tenacity.stop_after_attempt(8),
            wait=tenacity.wait_exponential_jitter(initial=1, exp_base=2, jitter=30),
            before_sleep=lambda retry_state: instance_logger.warning(
                f"Received 429, retrying in {retry_state.next_action.sleep} seconds...",
            ),
        ):
            with attempt:
                response = requests.get(endpoint, params=params)
                response.raise_for_status()
        content = response.json()

        # Save raw JSON
        # save_json_to_filesystem({"data": content}, resource_name, as_of_date, root_path)

        yield from content["data"]

    @dlt.source(name=f"jao_{resource_name}")
    def source(
        as_of_date: str = "2024-01-01",
        root_dir: Path | str = Path("data"),
        resource_logger: Optional[logging.Logger] = None,
    ) -> Any:
        """JAO data source for specific resource."""
        root_path = Path(root_dir)
        return resource(as_of_date, root_path, resource_logger)

    # Register the source
    _jao_sources[resource_name] = source
    return source


def _should_retry(retry_state) -> bool:
    """Determine if request should be retried based on response.

    Retries only on HTTP 429 (Too Many Requests) errors.
    """
    return (
        isinstance(retry_state.outcome.exception(), requests.exceptions.HTTPError)
        and retry_state.outcome.exception().response.status_code == 429
    )


# Create individual pipelines for each resource
max_net_position_source = create_jao_pipeline("max_net_position", "data/maxNetPos")
max_bex_source = create_jao_pipeline("max_bex", "data/maxExchanges")
initial_computation_source = create_jao_pipeline(
    "initial_computation", "data/initialComputation", {"presolved": "true"}
)
remedial_action_preventive_source = create_jao_pipeline(
    "remedial_action_preventive", "data/pra"
)
remedial_action_curative_source = create_jao_pipeline(
    "remedial_action_curative", "data/cra"
)
validation_reductions_source = create_jao_pipeline(
    "validation_reductions", "data/validationReductions"
)
pre_final_computation_source = create_jao_pipeline(
    "pre_final_computation", "data/preFinalComputation", {"presolved": "true"}
)
long_term_nomination_source = create_jao_pipeline("long_term_nomination", "data/ltn")
final_computation_source = create_jao_pipeline(
    "final_computation", "data/finalComputation", {"presolved": "true"}
)
lta_source = create_jao_pipeline("lta", "data/lta")
final_bex_source = create_jao_pipeline("final_bex_restrictions", "data/bexRestrictions")
allocation_constraints_source = create_jao_pipeline(
    "allocation_constraints", "data/allocationConstraint"
)
d2cf_source = create_jao_pipeline("d2cf", "data/d2CF")
refprog_source = create_jao_pipeline("refprog", "data/refprog")
reference_net_position_source = create_jao_pipeline(
    "reference_net_position", "data/referenceNetPosition"
)
atc_core_external_source = create_jao_pipeline("atc_core_external", "data/atc")
shadow_auction_atc_source = create_jao_pipeline(
    "shadow_auction_atc", "data/shadowAuctionAtc"
)
active_fb_constraints_source = create_jao_pipeline(
    "active_fb_constraints", "data/shadowPrices"
)
active_lta_constraints_source = create_jao_pipeline(
    "active_lta_constraints", "data/activeLtaConstraint"
)
congestion_income_source = create_jao_pipeline(
    "congestion_income", "data/congestionIncome"
)
scheduled_exchanges_source = create_jao_pipeline(
    "scheduled_exchanges", "data/scheduledExchanges"
)
net_position_source = create_jao_pipeline("net_position", "data/netPos")
intraday_atc_source = create_jao_pipeline("intraday_atc", "data/intradayAtc")
intraday_ntc_source = create_jao_pipeline("intraday_ntc", "data/intradayNtc")
price_spread_source = create_jao_pipeline("price_spread", "data/priceSpread")
spanning_dfp_source = create_jao_pipeline("spanning_dfp", "data/spanningDefaultFBP")
alpha_factor_source = create_jao_pipeline("alpha_factor", "data/alphaFactor")
