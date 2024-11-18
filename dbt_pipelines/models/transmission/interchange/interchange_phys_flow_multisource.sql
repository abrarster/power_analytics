SELECT COALESCE(a.bidding_zone_from, b.bidding_zone_from)          AS bidding_zone_from,
       COALESCE(a.bidding_zone_to, b.bidding_zone_to)              AS bidding_zone_to,
       COALESCE(a.for_date, b.for_date)                            AS for_date,
       a.value                                                     AS entsoe,
       b.value                                                     AS elia,
       COALESCE(a.value, b.value)                                  AS value,
       CASE WHEN a.value IS NOT NULL THEN 'entsoe' ELSE 'elia' END AS data_source
FROM {{ ref('entsoe_interchange_physical') }} a
         FULL OUTER JOIN {{ ref('elia_itc_phys_flow_clean') }} b ON a.bidding_zone_from = b.bidding_zone_from AND
                                                                    a.bidding_zone_to = b.bidding_zone_to AND
                                                                    a.for_date = b.for_date