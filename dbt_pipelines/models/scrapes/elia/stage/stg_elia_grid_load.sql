SELECT datetime::timestamptz as for_date,
       eliagridload as grid_load
FROM {{ source('elia', 'grid_load') }}
WHERE resolutioncode = 'PT15M'
