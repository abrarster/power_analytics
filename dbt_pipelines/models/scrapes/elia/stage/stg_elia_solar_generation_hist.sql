SELECT datetime::timestamptz as for_date,
       region,
       measured,
       mostrecentforecast,
       mostrecentconfidence10,
       mostrecentconfidence90,
       dayahead11hforecast,
       dayahead11hconfidence10,
       dayahead11hconfidence90,
       dayaheadforecast,
       dayaheadconfidence10,
       dayaheadconfidence90,
       weekaheadforecast,
       weekaheadconfidence10,
       weekaheadconfidence90,
       monitoredcapacity,
       loadfactor
FROM {{ source('elia', 'solar_generation_hist') }}
WHERE resolutioncode = 'PT15M'