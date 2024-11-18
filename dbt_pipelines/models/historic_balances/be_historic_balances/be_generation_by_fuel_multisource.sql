WITH
    elia_hourly as (
        SELECT 
            psr_type,
            date_trunc('hour', for_date) as for_date,
            avg(elia_rt_generation) as elia_rt_generation,
            avg(elia_da_generation) as elia_da_generation,
            avg(elia_rt_wind) as elia_rt_wind,
            avg(elia_rt_solar) as elia_rt_solar
        FROM {{ ref('elia_gen_byfuel_multisource') }}
        GROUP BY psr_type, date_trunc('hour', for_date)
    )
SELECT 'BE'::VARCHAR as bidding_zone,
       coalesce(a.for_date, b.for_date) as for_date,
       coalesce(a.psr_type, b.psr_type) as psr_type,
       a.generation_mw as entsoe,
       b.elia_rt_generation as elia_rt,
       b.elia_da_generation as elia_da,
       b.elia_rt_wind as elia_rt_wind,
       b.elia_rt_solar as elia_rt_solar
FROM {{ ref('entsoe_net_generation_by_fuel') }} a
    FULL OUTER JOIN elia_hourly b on a.for_date = b.for_date and 
                                     a.psr_type = b.psr_type
WHERE a.bidding_zone = 'BE'
ORDER BY coalesce(a.psr_type, b.psr_type), 
         coalesce(a.for_date, b.for_date)
