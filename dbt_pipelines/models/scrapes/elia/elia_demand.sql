SELECT coalesce(a.for_date, b.for_date) as for_date,
       a.grid_load,
       b.total_load
FROM {{ ref('stg_elia_grid_load') }} a
FULL OUTER JOIN {{ ref('stg_elia_total_load') }} b 
             ON a.for_date = b.for_date
ORDER BY coalesce(a.for_date, b.for_date)
