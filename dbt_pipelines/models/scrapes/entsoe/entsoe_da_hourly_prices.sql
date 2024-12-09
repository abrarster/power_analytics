SELECT *
FROM {{ ref('entsoe_da_prices_clean') }}
WHERE resolution = '60min'
