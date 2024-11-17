WITH base AS (
    SELECT 
        for_date,
        offshoreonshore,
        sum(measured) as generation,
        sum(monitoredcapacity) as capacity,
        sum(measured) / sum(monitoredcapacity) as loadfactor
    FROM {{ ref('stg_elia_wind_generation_hist') }}
    GROUP BY for_date, offshoreonshore
)

SELECT
    for_date,
    MAX(CASE WHEN offshoreonshore = 'Offshore' THEN generation END) as offshore_generation,
    MAX(CASE WHEN offshoreonshore = 'Offshore' THEN capacity END) as offshore_capacity,
    MAX(CASE WHEN offshoreonshore = 'Offshore' THEN loadfactor END) as offshore_loadfactor,
    MAX(CASE WHEN offshoreonshore = 'Onshore' THEN generation END) as onshore_generation,
    MAX(CASE WHEN offshoreonshore = 'Onshore' THEN capacity END) as onshore_capacity,
    MAX(CASE WHEN offshoreonshore = 'Onshore' THEN loadfactor END) as onshore_loadfactor
FROM base
GROUP BY for_date
ORDER BY for_date
