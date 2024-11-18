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
    ),
    merged as (
        SELECT 
            'BE'::VARCHAR as bidding_zone,
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
    )
SELECT 
    bidding_zone,
    psr_type,
    for_date,
    entsoe,
    elia_rt,
    elia_da,
    elia_rt_wind,
    elia_rt_solar,
    CASE psr_type 
        WHEN 'B01' THEN entsoe 
        WHEN 'B04' THEN coalesce(entsoe, elia_rt, elia_da) 
        WHEN 'B06' THEN coalesce(entsoe, elia_rt) 
        WHEN 'B10' THEN coalesce(entsoe, elia_rt, elia_da)
        WHEN 'B11' THEN entsoe
        WHEN 'B14' THEN coalesce(entsoe, elia_rt, elia_da)
        WHEN 'B16' THEN coalesce(entsoe, elia_rt_solar)
        WHEN 'B17' THEN entsoe
        WHEN 'B18' THEN coalesce(entsoe, elia_rt_wind, elia_rt)
        WHEN 'B19' THEN coalesce(entsoe, elia_rt_wind)
        WHEN 'B20' THEN coalesce(entsoe, elia_rt)
        ELSE NULL
    END as generation_mw,
    CASE psr_type 
        WHEN 'B01' THEN 'entsoe'
        WHEN 'B04' THEN CASE 
            WHEN entsoe IS NOT NULL THEN 'entsoe'
            WHEN elia_rt IS NOT NULL THEN 'elia_rt'
            ELSE 'elia_da'
        END
        WHEN 'B06' THEN CASE 
            WHEN entsoe IS NOT NULL THEN 'entsoe'
            ELSE 'elia_rt'
        END
        WHEN 'B10' THEN CASE 
            WHEN entsoe IS NOT NULL THEN 'entsoe'
            WHEN elia_rt IS NOT NULL THEN 'elia_rt'
            ELSE 'elia_da'
        END
        WHEN 'B11' THEN 'entsoe'
        WHEN 'B14' THEN CASE 
            WHEN entsoe IS NOT NULL THEN 'entsoe'
            WHEN elia_rt IS NOT NULL THEN 'elia_rt'
            ELSE 'elia_da'
        END
        WHEN 'B16' THEN CASE 
            WHEN entsoe IS NOT NULL THEN 'entsoe'
            ELSE 'elia_rt_solar'
        END
        WHEN 'B17' THEN 'entsoe'
        WHEN 'B18' THEN CASE 
            WHEN entsoe IS NOT NULL THEN 'entsoe'
            WHEN elia_rt_wind IS NOT NULL THEN 'elia_rt_wind'
            ELSE 'elia_rt'
        END
        WHEN 'B19' THEN CASE 
            WHEN entsoe IS NOT NULL THEN 'entsoe'
            ELSE 'elia_rt_wind'
        END
        WHEN 'B20' THEN CASE 
            WHEN entsoe IS NOT NULL THEN 'entsoe'
            ELSE 'elia_rt'
        END
    END as generation_source
FROM merged
ORDER BY psr_type, for_date
