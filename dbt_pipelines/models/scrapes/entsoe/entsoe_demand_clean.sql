SELECT a.out_bidding_zone_mrid as bidding_zone_code,
       b.name as bidding_zone,
       a.for_date::timestamptz as for_date,
       a.load_mw
FROM {{ source('entsoe', 'entsoe_demand') }} a
INNER JOIN {{ source('entsoe', 'entsoe_areas') }} b on a.out_bidding_zone_mrid = b.code
