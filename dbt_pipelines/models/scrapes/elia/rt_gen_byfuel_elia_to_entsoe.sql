{{ config(
    materialized='table',
    indexes=[
        {'columns': ['for_date']},
        {'columns': ['psr_type']},
        {'columns': ['for_date', 'psr_type']}
    ]
) }}

{%- set fuel_codes = dbt_utils.get_column_values(ref('stg_rt_gen_byfuel'), 'elia_fuel_code') -%}

WITH base_totals AS (
    SELECT 
        for_date,
        psr_type,
        SUM(generation) as total_generation
    FROM {{ ref('stg_rt_gen_byfuel') }}
    GROUP BY 1, 2
),
pivoted AS (
    SELECT 
        for_date,
        psr_type,
        {{ dbt_utils.pivot(
            'elia_fuel_code', 
            fuel_codes,
            agg='sum',
            then_value='generation'
        ) }}
    FROM {{ ref('stg_rt_gen_byfuel') }}
    GROUP BY 1, 2
)
SELECT 
    p.for_date, 
    p.psr_type,
    {% for col in fuel_codes %}
    "{{ col }}" as {{ col | lower | replace(' ', '_') }}{% if not loop.last %},{% endif %}
    {% endfor %},
    bt.total_generation
FROM pivoted p
JOIN base_totals bt ON bt.for_date = p.for_date AND bt.psr_type = p.psr_type
WHERE p.psr_type is not null
