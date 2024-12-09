SELECT coalesce(a.for_date, b.for_date) as for_date,
       a.load_mw as entsoe,
       b.grid_load as elia_grid_load,
       b.total_load as elia_total_load,
       coalesce(a.load_mw, b.total_load) as demand,
       CASE WHEN a.load_mw is not null
           THEN 'entsoe'
           ELSE 'elia_total_load'
       END as data_source
FROM {{ ref('entsoe_demand_clean') }} a
FULL OUTER JOIN {{ ref('elia_demand') }} b on a.for_date = b.for_date
WHERE a.bidding_zone = 'BE'
ORDER BY coalesce(a.for_date, b.for_date)