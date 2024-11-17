WITH 
    elia_rt_generation AS (
        SELECT for_date, psr_type, total_generation as elia_rt_generation
        FROM {{ ref('rt_gen_byfuel_elia_to_entsoe') }}
    ),
    elia_da_generation AS (
        SELECT for_date, psr_type, total_generation as elia_da_generation
        FROM {{ ref('da_gen_byfuel_elia_to_entsoe') }}
    ),
    elia_wind_generation AS (
        SELECT for_date,
              'B18' as psr_type,
               offshore_generation as generation
        FROM {{ ref('elia_wind_generation_hist_clean') }}
        UNION ALL
        SELECT for_date,
              'B19' as psr_type,
             onshore_generation as generation
        FROM {{ ref('elia_wind_generation_hist_clean') }} 
    ),
    elia_solar_generation AS (
        SELECT for_date,
              'B16' as psr_type,
               generation
        FROM {{ ref('elia_solar_generation_hist_clean') }}
    )
SELECT 
    COALESCE(a.psr_type, b.psr_type, c.psr_type, d.psr_type) as psr_type, 
    COALESCE(a.for_date, b.for_date, c.for_date, d.for_date) as for_date,
    a.elia_rt_generation, 
    b.elia_da_generation,
    c.generation as elia_rt_wind,
    d.generation as elia_rt_solar
FROM elia_rt_generation a
FULL OUTER JOIN elia_da_generation b 
    ON a.for_date = b.for_date AND a.psr_type = b.psr_type
FULL OUTER JOIN elia_wind_generation c 
    ON COALESCE(a.for_date, b.for_date) = c.for_date 
    AND COALESCE(a.psr_type, b.psr_type) = c.psr_type
FULL OUTER JOIN elia_solar_generation d 
    ON COALESCE(a.for_date, b.for_date, c.for_date) = d.for_date 
    AND COALESCE(a.psr_type, b.psr_type, c.psr_type) = d.psr_type
ORDER BY psr_type, for_date