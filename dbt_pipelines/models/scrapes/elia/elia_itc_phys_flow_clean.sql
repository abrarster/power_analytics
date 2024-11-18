WITH control_area_mapped AS (
    SELECT
        datetime :: timestamptz AS for_date,
        CASE
            controlarea
            WHEN 'France' THEN 'FR'
            WHEN 'Germany' THEN 'DE_LU'
            WHEN 'UnitedKingdom' THEN 'GB'
            WHEN 'Netherlands' THEN 'NL'
            WHEN 'Luxembourg' THEN 'DE_LU'
        END AS cp,
        physicalflowatborder AS VALUE
    FROM
        {{ source('elia', 'itc_phys_flow') }}
    WHERE
        resolutioncode = 'PT15M'
),
germany_aggregated AS (
    SELECT
        for_date,
        cp,
        SUM(VALUE) AS VALUE
    FROM
        control_area_mapped
    GROUP BY
        for_date,
        cp
),
hourly_downsampled AS (
    SELECT
        DATE_TRUNC(
            'hour',
            for_date
        ) AS for_date,
        cp,
        AVG(VALUE) AS VALUE
    FROM
        germany_aggregated
    GROUP BY
        DATE_TRUNC(
            'hour',
            for_date
        ),
        cp
)
SELECT
    'BE' :: VARCHAR AS bidding_zone_from,
    cp AS bidding_zone_to,
    for_date,
    VALUE
FROM
    hourly_downsampled
UNION ALL
SELECT
    cp AS bidding_zone_from,
    'BE' :: VARCHAR AS bidding_zone_to,
    for_date,- VALUE AS VALUE
FROM
    hourly_downsampled
