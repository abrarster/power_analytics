SELECT datetime::timestamptz as for_date,
       offshoreonshore,
       region,
       gridconnectiontype,
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
       loadfactor,
       decrementalbidid
FROM {{ source('elia', 'wind_generation_hist') }}
WHERE resolutioncode = 'PT15M'
    