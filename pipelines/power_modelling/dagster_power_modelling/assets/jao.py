import dagster
from datetime import date, timedelta
from typing import Optional, Callable
from pathlib import Path
from eupower_core.dagster_resources import FilesystemResource
from eupower_core.scrapes.jao.core import JaoFileClient, DataType
from .asset_groups import JAO


jao_partitions_def = dagster.DailyPartitionsDefinition(
    start_date="2023-01-01",
)


def make_jao_asset(data_type: DataType) -> Callable:
    """Factory function to create JAO assets for each endpoint.

    Args:
        data_type: The JAO endpoint to create an asset for

    Returns:
        A dagster asset function for the specified endpoint
    """

    @dagster.asset(
        name=f"jao_{data_type.endpoint.replace('/', '_').lower()}_raw",
        partitions_def=jao_partitions_def,
        group_name=JAO,
        kinds={"python", "file"},
    )
    def _asset(context: dagster.AssetExecutionContext, fs: FilesystemResource) -> None:
        """Download raw data from JAO {data_type.description} endpoint."""

        partition_date = date.fromisoformat(context.partition_key)
        writer = fs.get_writer(f"jao/{partition_date.strftime('%Y%m%d')}")
        base_path = writer.base_path
        client = JaoFileClient(custom_logger=context.log)
        context.log.info(f"Downloading {data_type.description} for {partition_date}")

        # Create folder path based on date: data/raw/jao/YYYY/MM/DD
        folder_path = Path(base_path) / partition_date.strftime("%Y/%m/%d")

        try:
            file_path = client.get_data(
                data_type=data_type,
                from_date=partition_date,
                to_date=partition_date,
                folder_path=folder_path,
                file_name=f"{data_type.endpoint.replace('/', '_').lower()}.json",
            )

            context.log.info(f"Successfully downloaded to {file_path}")

        except Exception as e:
            context.log.error(
                f"Failed to download {data_type.description} for {partition_date}: {str(e)}"
            )
            raise

    return _asset


jao_maxnetpositions_raw = make_jao_asset(DataType.MAX_NET_POSITIONS)
jao_maxexchanges_raw = make_jao_asset(DataType.MAX_EXCHANGES)
jao_initialcomputation_raw = make_jao_asset(DataType.INITIAL_COMPUTATION)
jao_pra_raw = make_jao_asset(DataType.REMEDIAL_ACTION_PREVENTIVE)
jao_cra_raw = make_jao_asset(DataType.REMEDIAL_ACTION_CURATIVE)
jao_validationreductions_raw = make_jao_asset(DataType.VALIDATION_REDUCTIONS)
jao_prefinalcomputation_raw = make_jao_asset(DataType.PRE_FINAL_COMPUTATION)
jao_ltn_raw = make_jao_asset(DataType.LONG_TERM_NOMINATION)
jao_finalcomputation_raw = make_jao_asset(DataType.FINAL_COMPUTATION)
jao_lta_raw = make_jao_asset(DataType.LTA)
jao_bexrestrictions_raw = make_jao_asset(DataType.FINAL_BILATERAL_EXCHANGE)
jao_allocationconstraint_raw = make_jao_asset(DataType.ALLOCATION_CONSTRAINTS)
jao_d2cf_raw = make_jao_asset(DataType.D2CF)
jao_refprog_raw = make_jao_asset(DataType.REFPROG)
jao_referencenetposition_raw = make_jao_asset(DataType.REFERENCE_NET_POSITION)
jao_atc_raw = make_jao_asset(DataType.ATC_CORE_EXTERNAL)
jao_shadowauctionatc_raw = make_jao_asset(DataType.SHADOW_AUCTION_ATC)
jao_shadowprices_raw = make_jao_asset(DataType.ACTIVE_FB_CONSTRAINTS)
jao_activeltaconstraint_raw = make_jao_asset(DataType.ACTIVE_LTA_CONSTRAINTS)
jao_congestionincome_raw = make_jao_asset(DataType.CONGESTION_INCOME)
jao_scheduledexchanges_raw = make_jao_asset(DataType.SCHEDULED_EXCHANGES)
jao_netpos_raw = make_jao_asset(DataType.NET_POSITION)
jao_intradayatc_raw = make_jao_asset(DataType.INTRADAY_ATC)
jao_intradayntc_raw = make_jao_asset(DataType.INTRADAY_NTC)
jao_pricespread_raw = make_jao_asset(DataType.PRICE_SPREAD)
jao_spanningdefaultfbp_raw = make_jao_asset(DataType.SPANNING_DFP)
jao_alphafactor_raw = make_jao_asset(DataType.ALPHA_FACTOR)
