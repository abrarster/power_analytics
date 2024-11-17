SELECT 
    for_date,
    measured as generation,
    monitoredcapacity as capacity,
    loadfactor
FROM {{ ref('stg_elia_solar_generation_hist') }}
WHERE region = 'Belgium'
