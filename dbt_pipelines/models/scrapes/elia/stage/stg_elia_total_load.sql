SELECT datetime::timestamptz as for_date,
       totalload as total_load
FROM {{ source('elia', 'total_load') }}
WHERE resolutioncode = 'PT15M'

