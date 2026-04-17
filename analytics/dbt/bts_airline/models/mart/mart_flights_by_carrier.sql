-- Mart: total and diverted flight counts per airline.

{{ config(
    materialized='table'
)}}

with staging_data as (
    select * from {{ref('stg_carrier_report')}}
),

aggregated_flights as (
    select
        reporting_airline,
        airline_name,
        count(*) as total_flights,

        sum(cast(diverted as int64)) as total_diverted_flights
    from staging_data
    group by
        reporting_airline,
        airline_name
)

select
    reporting_airline,
    airline_name,
    total_flights,
    total_diverted_flights,
    
    -- Calculate diversion rate using SAFE_DIVIDE to avoid division by zero errors
    round(safe_divide(total_diverted_flights, total_flights) * 100, 2) as diverted_rate_pct

from aggregated_flights
-- Largest airlines first
order by total_flights desc