SELECT a.datetime::timestamptz as for_date,
       a.fuelcode as elia_fuel_code,
       b.psr_type,
       a.dayaheadgenerationschedule as generation
FROM {{ source("elia", "da_gen_byfuel") }} a
         LEFT JOIN {{ source("mapping_tables", "elia_fuel_codes") }} b on a.fuelcode = b.elia_fuel_code
WHERE a.resolutioncode = 'PT15M'
