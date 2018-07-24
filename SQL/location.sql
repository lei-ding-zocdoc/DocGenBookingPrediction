-- Create a temporary table to map provider_location_id to geographic information
with provider as (
    select distinct
        last_day(day) as month_end,
        providerid,
        locationid
    from experimental_ext.historical_provider_location_mapping_by_day
    where last_day(day) >= '2017-01-01'
),



location as (
    select
        provider.month_end,
        provider.providerid,
        provider.locationid,
        mapping.zipcode,
        mapping.countyname,
        mapping.city,
        mapping.msa_city,
        mapping.state
    from provider
        left join provider.location as location
        on provider.locationid = location.location_id
        left join experimental.zipcode_mapping_ds AS mapping
        on LEFT(location.zip_code, 5) = mapping.zipcode
)


select *
from location
;
