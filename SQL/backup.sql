with spec as (
select spec.monolith_specialty_id, spec.specialty_id
from provider.specialty as spec
group by spec.monolith_specialty_id, spec.specialty_id
),



appts as (
select   docs.provider_id || '|' || ac.location_id as providerlocationid
       , spec_cat.specialty_category
       , docs.monolith_professional_id
       , docs.monolith_provider_id
       , (docs.strategic_id is null or docs.strategic_id = 248)::integer as is_local_or_midmarket
       , docs.activation_time_utc
       , case ac.zip
            when '01655' then '01604' when '10020' then '10019' when '10107' then '10019' when '10111' then '10019'
            when '10123' then '10001' when '10155' then '10022' when '10166' then '10169' when '10168' then '10017'
            when '10169' then '10017' when '10172' then '10017' when '10174' then '10017' when '10176' then '10017'
            when '10177' then '10017' when '10270' then '10007' when '10271' then '10005' when '10279' then '10007'
            when '11242' then '11201' when '11243' then '11217' when '17822' then '17821' when '18711' then '18701'
            when '18765' then '18702' when '33151' then '33127' when '46282' then '46204' when '77574' then '77573'
            else ac.zip
         end as doctor_zip
       , ac.msa as doctor_msa
       , ac.appointment_id
       , ac.appointment_creation_timestamp_utc
       , ac.location_id
       , docs.provider_id
from provider.provider as docs
left join spec on docs.monolith_main_specialty_id = spec.monolith_specialty_id
left join sales_ops.flapjack_pricing_specialty_categorization as spec_cat
  on coalesce(spec.specialty_id, json_extract_array_element_text(docs.approved_specialty_ids, 0)) = spec_cat.specialty_id
left join appointment.appointment_created as ac
on docs.monolith_professional_id = ac.monolith_professional_id
where (docs.is_test is null or not docs.is_test)
  and (docs.is_bookable is null or docs.is_bookable)
  and (docs.can_perform_procedures is null or docs.can_perform_procedures)
  and docs.activation_time_utc >= '2017-01-01'
  and ac.appointment_creation_timestamp_utc >= '2017-05-01'
),



-- this table only goes back to 2018-03-01
availability as (
select   a.monolith_professional_id
       , avg(a."5_day_num_hours_with_availability") as availability
from experimental.daily_availability_summary as a
group by a.monolith_professional_id
),




doctor_density as (
select   a.doctor_zip
       , count(distinct a.monolith_professional_id) as num_docs_in_zip
from appts as a
group by a.doctor_zip
)



select   a.monolith_professional_id as doctor_id
       , a.location_id
       , date_trunc('month', a.appointment_creation_timestamp_utc) as appt_month
       -- TODO: days on site? (look into a.activation_time_utc)
       , a.specialty_category
       -- TODO: number of specialties?
       , a.is_local_or_midmarket
       , case a.doctor_zip
            when '11249' then 'New York-Northern New Jersey-Long Island, NY-NJ-PA Metropolitan Statistical Area'
            when '11201' then 'New York-Northern New Jersey-Long Island, NY-NJ-PA Metropolitan Statistical Area'
            when '75033' then 'Dallas-Fort Worth-Arlington, TX Metropolitan Statistical Area'
            when '12775' then 'New York-Northern New Jersey-Long Island, NY-NJ-PA Metropolitan Statistical Area'
            when '18701' then 'Scranton--Wilkes-Barre, PA Metropolitan Statistical Area'
            when '85378' then 'Phoenix-Mesa-Scottsdale, AZ Metropolitan Statistical Area'
            when '33127' then 'Miami-Fort Lauderdale-Pompano Beach, FL Metropolitan Statistical Area'
            when '46204' then 'Indianapolis-Carmel, IN Metropolitan Statistical Area'
            else a.doctor_msa
         end as doctor_msa
       , a.doctor_zip
       -- TODO: # doc-gen appts
       -- TODO: # doc-gen premium appts
       , count(distinct a.appointment_id) as num_total_appts
       -- TODO: # docs in practice
       -- TODO: days on site for practice
       -- TODO: city, state, county
       , avg(availability) as availability -- TODO: pull in all columns instead of just 5-day
       , sum(dd.num_docs_in_zip) as num_docs_in_zip
from appts as a
left join availability as av on a.monolith_professional_id = av.monolith_professional_id
left join doctor_density as dd on a.doctor_zip = dd.doctor_zip
group by 1, 2, 3, 4, 5, 6, 7;