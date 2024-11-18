SELECT bidding_zone_code,
       bidding_zone,
       psr_type,
       fuel,
       for_date::timestamptz,
       sum(generation_mw) as generation_mw
FROM {{ source('entsoe', 'fct_entsoe_generation_by_fuel') }}
group by bidding_zone_code, bidding_zone, psr_type, fuel, for_date::timestamptz
