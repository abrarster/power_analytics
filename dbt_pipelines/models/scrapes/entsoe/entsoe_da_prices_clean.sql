SELECT a.bidding_zone as bidding_zone_code,
       b.name as bidding_zone,
       resolution,
       currency,
       unit,
       for_date::timestamptz as for_date,
       price
FROM {{ source('entsoe', 'entsoe_da_prices') }} a
INNER JOIN {{ source('entsoe', 'entsoe_areas') }} b on a.bidding_zone = b.code
