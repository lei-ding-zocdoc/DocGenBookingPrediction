-- create a temporary table for all the month start day
with month as (
    select distinct last_day(d.today) as month_end
    from experimental.t_dim_dates as d
        where d.today >= '2017-01-01'  -- only consider after 2017-01-01
        and d.today < date_trunc('month', CURRENT_DATE)   -- don't consider current month
)
,



active_doctor_by_day as (
    select
        d.today as date,
        s.professionalsrcid,
        p.provider_id
    from experimental.t_dim_dates as d
        left join datadictionary.tableau_statuschanges as s
            on s.professionalstatusstartdate <= d.today
            and s.professionalstatusenddate >= d.today
        left join provider.provider as p
            on s.professionalsrcid = p.monolith_professional_id
    where d.today >= '2017-01-01'  -- only consider after 2017-01-01
        and d.today < date_trunc('month', CURRENT_DATE)   -- don't consider current month
        and s.isactivated = TRUE
    order by 3, 1
)
,




days_on_site_this_month as (
    select
        month.month_end,
        d.provider_id,
        count(d.date) as days
    from month
        left join active_doctor_by_day as d
        on d.date <= month.month_end
        and d.date >= date_trunc('month', month.month_end)
    group by 1, 2
)
,




days_on_site_overall as (
    select
        month.month_end,
        d.provider_id,
        count(d.date) as days
    from month
        left join active_doctor_by_day as d
        on d.date <= month.month_end
    group by 1, 2
)


-- Create a temporary table to provide info about active doctors for each month
-- active_doctor_by_month as (
    select
        m.month_end,
        m.provider_id,
        m.days as days_on_site_this_month,
        o.days as days_on_site_overall,
        count(distinct a.appointment_id) as num_appt,

        count(distinct
            case
                when a.is_premium_eligible = TRUE then a.appointment_id
            end
        ) as num_appt_premium_eligible,

        count(distinct
            case
                when a.is_provider_owned_property = TRUE then a.appointment_id
            end
        ) as num_appt_provider_owned,

        count(distinct
            case
                when p.be_user_intent_category = 'doctor-generated' then a.appointment_id
            end
        ) as num_appt_doc_gen,

        count(distinct
            case
                when p.be_user_intent_category = 'doctor-generated' and a.is_premium_eligible = TRUE then a.appointment_id
        end
        ) as num_doc_doc_gen_premium

    from days_on_site_this_month as m
        left join days_on_site_overall as o
            on m.month_end = o.month_end
            and m.provider_id = o.provider_id
        left join appointment.appointment_created as a
            on m.provider_id = a.provider_id
            and last_day(a.appointment_creation_timestamp_utc) = m.month_end
        left join metrics.session_performance as p
            on a.session_id = p.session_id
    group by 1, 2, 3, 4
    order by 2, 1
    limit 100
-- )





-- with doctor AS (
--   SELECT
--     weekstart.today AS date,
--     temp_specialty.specialty_category,
--     temp_location.city,
--     COUNT(DISTINCT CASE WHEN doc.salesteam = 'Local' THEN doc.professionalsrcid END) as num_doc_local,
--     COUNT(DISTINCT CASE WHEN doc.salesteam ='Affiliate' OR salesteam = 'Health Systems' THEN doc.professionalsrcid END) as num_doc_hs,
--     COUNT(DISTINCT doc.professionalsrcid) AS num_doc
--   FROM
--     experimental.t_dim_dates AS weekstart
--     LEFT JOIN datadictionary.tableau_statuschanges AS doc
--       ON doc.professionalstatusstartdate <= weekstart.today
--          AND doc.professionalstatusenddate >= weekstart.today
--     LEFT JOIN provider.provider AS provider
--       ON doc.professionalsrcid = provider.monolith_professional_id
--     LEFT JOIN temp_specialty
--       ON COALESCE(provider.monolith_main_specialty_id, CAST(TRIM('"]' FROM TRIM('["sp_' FROM provider.approved_specialty_ids)) AS BIGINT)) = temp_specialty.specialty_id
--     LEFT JOIN temp_location_mapping AS location_mapping
--         ON provider.provider_id = location_mapping.provider_id
--     LEFT JOIN temp_location
--         ON location_mapping.location_id = temp_location.location_id
--   WHERE doc.isactivated = TRUE
--   GROUP BY 1, 2, 3
-- )
--
