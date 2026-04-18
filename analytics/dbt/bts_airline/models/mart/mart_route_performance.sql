{{ config(
    materialized='table'
) }}

with flight_data as (
    select * from {{ ref('stg_carrier_report') }}
),

route_metrics as (
    select
        origin,
        origin_city_name,
        dest,
        dest_city_name,
        count(*) as total_flights,
        
        -- Average air time for this specific route
        round(avg(air_time), 2) as avg_air_time_mins,
        
        -- Total cancellations on this route
        sum(cast(cancelled as int64)) as total_cancelled,
        
        -- Average delays
        round(avg(dep_delay), 2) as avg_dep_delay_mins,
        round(avg(arr_delay), 2) as avg_arr_delay_mins

    from flight_data
    group by 
        origin,
        origin_city_name,
        dest,
        dest_city_name
)

select
    origin,
    origin_city_name,
    dest,
    dest_city_name,
    total_flights,
    avg_air_time_mins,
    total_cancelled,
    
    -- Calculate cancellation rate safely
    round(safe_divide(total_cancelled, total_flights) * 100, 2) as cancellation_rate_pct,
    
    avg_dep_delay_mins,
    avg_arr_delay_mins

from route_metrics
-- Order by the busiest routes first
order by total_flights desc