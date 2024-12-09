WITH
    region_mapped as (
        SELECT c.name                  AS bidding_zone_from,
               b.name                  AS bidding_zone_to,
               a.for_date::timestamptz AS for_date,
               a.value
        FROM {{ source('entsoe', 'entsoe_crossborder_flows') }} a
                LEFT JOIN {{ source('entsoe', 'entsoe_areas') }} b ON a.in_domain = b.code
                LEFT JOIN {{ source('entsoe', 'entsoe_areas') }} c ON a.out_domain = c.code
    ),
    hourly_downsampled as (
        SELECT bidding_zone_from,
               bidding_zone_to,
               date_trunc('hour', for_date) as for_date,
               avg(value) as value
        FROM region_mapped
        GROUP BY bidding_zone_from, bidding_zone_to, date_trunc('hour', for_date)    
    ),
    stacked as (
        SELECT bidding_zone_from,
               bidding_zone_to,
               for_date,
               value
        FROM hourly_downsampled
        UNION ALL
        SELECT bidding_zone_to   AS bidding_zone_from,
               bidding_zone_from AS bidding_zone_to,
               for_date,
               -value            AS value
        FROM hourly_downsampled
    )
SELECT 'physical_flow' as flow_type,
       bidding_zone_from,
       bidding_zone_to,
       for_date,
       SUM(value) AS value
FROM stacked
GROUP BY bidding_zone_from, bidding_zone_to, for_date
