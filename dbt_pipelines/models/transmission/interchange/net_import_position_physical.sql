SELECT bidding_zone_from, 
       for_date, 
       -sum(value) as value
FROM {{ ref('interchange_phys_flow_multisource') }}
GROUP BY bidding_zone_from, for_date