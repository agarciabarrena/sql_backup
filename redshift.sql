--- SETTLEMENT REPORT settleement report
with tran as (
select
    date_trunc('month', convert_timezone('Europe/Paris',timestamp)) as mes,
       gateway,
    operator_code,
    tariff,
    coalesce(count(rockman_id), 0) as nr_transactions_trans_table
from
     revenue
where
    timestamp between '2021-10-01 00:00:00' and '2021-10-31  11:59:59'
  and timestamp < current_date
  and country_code = 'FR'
  and dnstatus = 'Delivered'
  and tariff > '0'
group by 1, 2, 3, 4),

rev as (
select
    date_trunc('month', convert_timezone('Europe/Paris',timestamp)) as mes,
    gateway,
    operator_code,
    tariff,
    coalesce(count(rockman_id), 0) as nr_transactions_revenue,
    sum(coalesce(original_revenue, 0)) as original_revenue,
    sum(coalesce(original_local_currency_revenue, 0)) as original_local_curr_revenue,
    sum(coalesce(revenue, 0)) as revenue,
    sum(coalesce(local_currency_revenue, 0)) as local_curr_revenue
from
    revenue
where
    timestamp between '2021-10-01 00:00:00' and '2021-10-31 11:59:59'
    and timestamp < current_date
    and country_code = 'FR'
    and dnstatus = 'Delivered'
    and tariff > '0'
group by 1, 2, 3, 4)

select
    to_char(trunc(t.mes), 'YYYY-MM-DD') as date,
    t.gateway,
    t.operator_code,
    t.tariff,
    sum(coalesce(t.nr_transactions_trans_table, 0)) as nr_transactions,
    sum(coalesce(r.nr_transactions_revenue, 0)) as nr_transactions_rev,
    sum(coalesce(r.original_revenue, 0)) as original_revenue,
    sum(coalesce(r.original_local_curr_revenue, 0)) as original_local_curr_revenue,
    sum(coalesce(r.revenue, 0)) as revenue,
    sum(coalesce(r.local_curr_revenue, 0)) as local_curr_revenue
from tran t
    left join rev r on t.mes = r.mes
                                   and t.tariff = r.tariff
                                   and t.gateway = r.gateway
                                   and t.operator_code = r.operator_code
group by 1, 2, 3,4
order by 1, 2, 3, 4




-- USING SPECTRUM
select *
from spectrum.pacman
where rockman_id='c35e4a101b6711eb970781ecd1eecffa'
  and year=2020
and month='10'


-- A/ B test
select distinct (msisdn), ab_test, handle_name from user_sessions
where country_code='RS'
and timestamp between '2021-02-16' and '2021-02-22'
  and sale > 0
and ab_test = 'o:rs:RS-video-tallyman.v1-click2sms-nomodal-keywordPLAY-abtest:gamiclub-1556-play'
  and handle_name = 'o:rs:RS-video-tallyman.v1-click2sms-nomodal-keywordPLAY-abtest:gamiclub-1556-play'


-- TEST data points
select * from pacman
where rockman_id = '29d1b44022a511ec95abc741a6fd03f3'
and country_code = 'ES'
and timestamp >= '2021-10-01'

select * from content_usage
where rockman_id = '29d1b44022a511ec95abc741a6fd03f3'
and timestamp >= '2021-10-01'

select * from transactions
where rockman_id = '29d1b44022a511ec95abc741a6fd03f3'
and country_code = 'ES'
and timestamp >= '2021-10-01'




insert into customer_care_flow_events values ('00007', 'imp_id', '2021-07-01', 'test', 'feedback_forecasted',
                                              '{"msisdn": true, "feedback": "el producto no me gusta", "reason": "I subscribed by accident", "lang": "es", "translated_feedback": "I dont like the product", "risk_score": 7}',
                                              1988.0, '2021-07-01')


--flow FLOW
select
       date_trunc('day', timestamp) as day,
       sum(case when event_type = 'impression' then 1 else 0 end) as impression,
       --sum(case when event_type = 'lead1' then 1 else 0 end) as lead,
       sum(case when event_type = 'validmsisdn' then 1 else 0 end) as validmsisdn,
       sum(case when event_type = 'invalidmsisdn' then 1 else 0 end) as invalidmsisdn,
       sum(case when event_type = 'block' then 1 else 0 end) as block,
       sum(case when event_type = 'pinsent' then 1 else 0 end) as pinsent,
       sum(case when event_type = 'validpin' then 1 else 0 end) as validpin,
       sum(case when event_type = 'invalidpin' then 1 else 0 end) as invalidpin,
       sum(case when event_type = 'missing_config' then 1 else 0 end) as missing_config,
       sum(case when event_type = 'redirect_api_success' then 1 else 0 end) as redirect_api_success,
       sum(case when event_type = 'gateway_api_subscription_mt_success' then 1 else 0 end) as gateway_subscription_mt_success,
       sum(case when event_type = 'gateway_api_subscription_mt_fail' then 1 else 0 end) as gateway_subscription_mt_fail,
       sum(case when event_type = 'notification_subscription_confirm_mt_success' then 1 else 0 end) as notification_subscription_confirm_mt_success,
       sum(case when event_type = 'gateway_api_unsubscription_success' then 1 else 0 end) as gateway_unsubscription_success,
       sum(case when event_type = 'resubscribe' then 1 else 0 end) as resubscribe,
       sum(case when event_type = 'sale' then 1 else 0 end) as sale,
       sum(case when event_type = 'firstbilling' then 1 else 0 end) as firstbilling
from pacman where country_code='RS'
and timestamp between '2021-12-26 00:00' and '2021-12-30 23:59'
--and affiliate_id = 'WDG'--ADL ASM
group by 1
order by 1 asc


with imp as (
    select
    us.country_code
    , us.google_placement_name as placement_name
    , json_extract_path_text(us.query_string, 'g_adgroupid') as ad_group_id
    , json_extract_path_text(us.query_string, 'gclid') as gclid
    , convert_timezone('UTC', 'Europe/Amsterdam', us.timestamp)::date as date
    , sum(case when us.impression > 0 or us.redirect > 0 then 1 else 0 end) as lp_impressions
    , sum(case when us.sale > 0 then 1 else 0 end) as sales
    , sum(case when us.sale > 0 and us.sale_timestamp < current_date - interval '8 day' then 1 else 0 end) as sales_week_1
    , sum(case when us.sale > 0 and us.sale_timestamp < current_date - interval '15 day' then 1 else 0 end) as sales_week_2
    , sum(case when us.sale > 0 and us.sale_timestamp < current_date - interval '22 day' then 1 else 0 end) as sales_week_3
    , sum(case when us.sale > 0 and us.sale_timestamp < add_months(current_date, -1) - interval '1 day' then 1 else 0 end) as sales_month_1
    , sum(case when us.sale > 0 and us.sale_timestamp < add_months(current_date, -2) - interval '1 day' then 1 else 0 end) as sales_month_2
    , sum(case when us.sale > 0 and us.sale_timestamp < add_months(current_date, -3) - interval '1 day' then 1 else 0 end) as sales_month_3
    , sum(case when us.sale > 0 and us.sale_timestamp < add_months(current_date, -4) - interval '1 day' then 1 else 0 end) as sales_month_4
    , sum(case when us.sale > 0 and us.sale_timestamp < add_months(current_date, -5) - interval '1 day' then 1 else 0 end) as sales_month_5
    , sum(case when us.sale > 0 and us.sale_timestamp < add_months(current_date, -6) - interval '1 day' then 1 else 0 end) as sales_month_6
    from user_sessions us
    where
    us.country_code = 'AE'
    and us.timestamp >= convert_timezone('UTC', 'Europe/Amsterdam', '2021-02-28 00:00:00')
    and us.timestamp < convert_timezone('UTC', 'Europe/Amsterdam', '2021-08-30 00:00:00')
    and us.google_placement_name is not null
    and us.google_placement_name <> ''
    and us.query_string like '%g_adgroupid%'
    and ad_group_id <> ''
    and us.query_string like '%gclid%'
    and gclid <> ''
    and us.form_factor <> 'Robot'
    group by 1,2,3,4,5
    order by 1,2,3,4,5
    ),
subs as(
    select
    ub.country_code
    , ub.google_placement_name as placement_name
    , json_extract_path_text(ub.query_string, 'g_adgroupid') as ad_group_id
    , json_extract_path_text(ub.query_string, 'gclid') as gclid
    , convert_timezone('UTC', 'Europe/Amsterdam', ub.timestamp)::date as date
    , coalesce(sum(ub.tb_first_week_revenue), 0)::float as revenue_week_1
    , coalesce(sum(ub.tb_first_week_revenue + ub.tb_second_week_revenue), 0)::float as revenue_week_2
    , coalesce(sum(ub.tb_first_week_revenue + ub.tb_second_week_revenue + ub.tb_third_week_revenue), 0)::float as revenue_week_3
    , coalesce(sum(ub.tb_first_month_revenue), 0)::float as revenue_month_1
    , coalesce(sum(ub.tb_first_month_revenue + ub.tb_second_month_revenue), 0)::float as revenue_month_2
    , coalesce(sum(ub.tb_three_month_revenue), 0)::float as revenue_month_3
    , coalesce(sum(ub.tb_three_month_revenue + ub.tb_4th_month_revenue), 0)::float as revenue_month_4
    , coalesce(sum(ub.tb_three_month_revenue + ub.tb_4th_month_revenue + ub.tb_5th_month_revenue), 0)::float as revenue_month_5
    , coalesce(sum(ub.tb_three_month_revenue + ub.tb_4th_month_revenue + ub.tb_5th_month_revenue + ub.tb_6th_month_revenue), 0)::float as revenue_month_6
    from user_subscriptions ub
    where
    ub.country_code = 'AE'
    and ub.timestamp >= convert_timezone('UTC', 'Europe/Amsterdam', '2021-02-28 00:00:00')
    and ub.timestamp < convert_timezone('UTC', 'Europe/Amsterdam', '2021-08-30 00:00:00')
    and ub.google_placement_name is not null
    and ub.google_placement_name <> ''
    and ub.query_string like '%g_adgroupid%'
    and ad_group_id <> ''
    and ub.query_string like '%gclid%'
    and gclid <> ''
    and ub.form_factor <> 'Robot'
    group by 1,2,3,4,5
    order by 1,2,3,4,5
    )
select imp.country_code,
imp.placement_name,
imp.ad_group_id,
imp.gclid,
imp.date,
imp.lp_impressions,
coalesce(sales, 0) as sales,
coalesce(sales_week_1, 0) as sales_week_1,
coalesce(sales_week_2, 0) as sales_week_2,
coalesce(sales_week_3, 0) as sales_week_3,
coalesce(sales_month_1, 0) as sales_month_1,
coalesce(sales_month_2, 0) as sales_month_2,
coalesce(sales_month_3, 0) as sales_month_3,
coalesce(sales_month_4, 0) as sales_month_4,
coalesce(sales_month_5, 0) as sales_month_5,
coalesce(sales_month_6, 0) as sales_month_6,
coalesce(revenue_week_1, 0) as revenue_week_1,
coalesce(revenue_week_2, 0) as revenue_week_2,
coalesce(revenue_week_3, 0) as revenue_week_3,
coalesce(revenue_month_1, 0) as revenue_month_1,
coalesce(revenue_month_2, 0) as revenue_month_2,
coalesce(revenue_month_3, 0) as revenue_month_3,
coalesce(revenue_month_4, 0) as revenue_month_4,
coalesce(revenue_month_5, 0) as revenue_month_5,
coalesce(revenue_month_6, 0) as revenue_month_6
from imp left join subs
    on imp.country_code = subs.country_code
        and imp.placement_name = subs.placement_name
        and imp.ad_group_id = subs.ad_group_id
        and imp.gclid = subs.gclid
        and imp.date = subs.date


--does instant churned users access portal?
select distinct rockman_id from content_usage
where rockman_id in (select rockman_id
                        from user_subscriptions
                        where country_code='FR'
                          and optout_timestamp-user_subscriptions.sale_timestamp < interval '1 day')
and timestamp >= '2021-08-15'



-- special case of the min lifetime user
select optout_reason
from user_subscriptions
where country_code='FR'
  and rockman_id = 'ae88eae0034811ecbe76a7f7ec60b23b'
-- /FRANCE


--check placements code
select placement_name, sum(ad_impressions), sum(ad_clicks), sum(conversions) from dev.placement_test group by 1



-- ES Churn rate
select msisdn,
       sale_timestamp,
       optout_timestamp,
       coalesce(extract(seconds from optout_timestamp-sale_timestamp),extract(seconds from current_date-sale_timestamp)) as churn_seconds,
       round(coalesce(extract(seconds from optout_timestamp-sale_timestamp),extract(seconds from current_date-sale_timestamp))/ (24*3600)) as churn_days,
       case when optout_timestamp-sale_timestamp >= interval '8 days 1 hour' then 0 when optout_timestamp is null then 2 else 1 end as instant_churn,
       extract('hour' from convert_timezone('UTC', 'Europe/Madrid', sale_timestamp)) as sale_hour,
       json_extract_path_text(query_string, 'g_adgroupid') as g_adgroup,
       handle_name,
       google_placement_name,
       user_id,
       coalesce(optout_reason, 'still_subscribed') as optout_reason
from user_subscriptions
where country_code='ES'
  and sale_timestamp between '2021-03-01' and '2021-04-01'




-- GR Check billing users
with usr as (select rockman_id,
                    sale_timestamp,
                    optout_timestamp,
                    case
                        when (optout_timestamp is not null) and (optout_timestamp <= sale_timestamp + interval '31 days') then optout_timestamp
                        else sale_timestamp + interval '31 days' end as end_first_month_timestamp
             from user_subscriptions
             where country_code = 'GR'
               and sale_timestamp between '2021-08-01' and '2021-09-01'),

     rev as (select rockman_id,
                    revenue.timestamp as billing_timestamp,
                    (tariff::float / 100)::float as billed
                from revenue
                where
                    country_code = 'GR'
                    and timestamp between '2021-08-01' and '2021-10-01')
select usr.rockman_id,
       sum(rev.billed::float)
from usr left join rev on usr.rockman_id=rev.rockman_id
                              and billing_timestamp < usr.end_first_month_timestamp
group by 1
    having sum(rev.billed::float) is not null
order by 2 desc;

select sale_timestamp, --2021-08-02 13:19:13.000000
       optout_timestamp -- null
from user_subscriptions where rockman_id ='3cd7ce30ff7311eb84a717f55c811fc2'--'b4180ff0f39311ebb1d2ef9477b563ee'

select date_trunc('day', revenue.timestamp),
       sum((tariff::float / 100)::float) as billed
from revenue
where rockman_id ='3cd7ce30ff7311eb84a717f55c811fc2'
group by 1


-- CHECK SUBS ANALYSYS
    select
        customer_care_id,
        insert_timestamp,
        json_extract_path_text(args, 'msisdn') msisdn,
        json_extract_path_text(args, 'rockman_id') as rockman_id,
        json_extract_path_text(args, 'reason') as reason,
        json_extract_path_text(args, 'language') as language,
        json_extract_path_text(args, 'message') as message,
        json_extract_path_text(args, 'message_translated') as message_translated,
        json_extract_path_text(args, 'risk_score')as risk_score
    from customer_care_flow_events cce
    where cce.action = 'feedback_forecasted'
            and cce.timestamp >= '2021-05-28'
            and json_extract_path_text(cce.args, 'rockman_id') != '';

-- checksubscription
select *
from customer_care_flow_events cce
where cce.action = 'feedback_forecasted'
        and cce.insert_timestamp >= '2021-10-14'
        and json_extract_path_text(cce.args, 'rockman_id') != ''



select rockman_id from user_subscriptions
where timestamp >= '2021-01-01' and country_code = 'GR'
and optout_timestamp is not null



select msisdn, affiliate_id, pubid, sub_id
from user_subscriptions
where msisdn = '40743790879'
  and country_code='RO'
  and timestamp >= '2021-09-01'


select
    to_char(date_trunc('month', sale_timestamp), 'YYYY-MM-DD') as month
  , sum(us.home_cpa) as total_cost
  , sum(case when us.sale > 0 then 1 else null end) :: float as sales
  , sum(case when us.sale > 0 and date_diff('week', date_trunc('week', us.sale_timestamp), date_trunc('week', current_date)) >= 2  then us.tb_first_week_revenue else null end) :: float as revenue_week_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 2  then us.tb_first_month_revenue else null end) :: float as revenue_month_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 3  then us.tb_first_month_revenue + us.tb_second_month_revenue else null end) :: float as revenue_month_2
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 4  then us.tb_three_month_revenue else null end) :: float as revenue_month_3
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 5  then us.tb_three_month_revenue + us.tb_4th_month_revenue else null end) :: float as revenue_month_4
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 6  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue else null end) :: float as revenue_month_5
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 7  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue else null end) :: float as revenue_month_6
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 8  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue else null end) :: float as revenue_month_7
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 9  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue else null end) :: float as revenue_month_8
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 10 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue else null end) :: float as revenue_month_9
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 11 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue else null end) :: float as revenue_month_10
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 12 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue else null end) :: float as revenue_month_11
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 13 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue + us.tb_12th_month_revenue else null end) :: float as revenue_month_12
from user_subscriptions us
where us.sale_timestamp between '2020-10-14' and '2021-10-14'
    and us.country_code = 'EG'
group by 1
order by 1

select date_trunc('day', sale_timestamp) as day, count(*)
from user_subscriptions where country_code='GR' and timestamp >= '2021-10-15'
group by 1


create table dev.icecream_all_countries
(month 		varchar,
ecpa     	float,
sales     	float,
arpu_week_1 float,
arpu_1    	float,
arpu_2    	float,
arpu_3    	float,
arpu_4    	float,
arpu_5    	float,
arpu_6    	float,
arpu_7    	float,
arpu_8    	float,
arpu_9    	float,
arpu_10    	float,
arpu_11    	float,
arpu_12    	float,
country_code varchar,
insert_timestamp timestamp)

create table dev.icecream_performance
(
cohort      varchar,
mean_difference        float,
mean_pct_difference      float,
median              float,
country_code      varchar)

delete from dev.icecream_performance
drop table dev.icecream_test



with check_subs as (select split_part(json_extract_path_text(cce.args, 'rockman_id'), '\\\"', 2)  as rockman_id,
                           json_extract_path_text(cce.args, 'message_translated') as message_translated,
                           json_extract_path_text(cce.args, 'risk_score')         as risk_score
                    from customer_care_flow_events cce
                    where category = 'forecast'
                      and insert_timestamp between '2021-10-19' and current_date)
select us.country_code
     , cs.*
from user_subscriptions us right join check_subs cs on us.rockman_id = cs.rockman_id
and us.timestamp between '2019-10-19' and current_date



select split_part(json_extract_path_text(cce.args, 'rockman_id'), ',')          as rockman_id
                    from customer_care_flow_events cce
                    where category = 'forecast'
                      and insert_timestamp between '2021-10-19' and current_date


select split_part(json_extract_path_text(cce.args, 'rockman_id'), '\\\"', 2)
from customer_care_flow_events cce
                    where category = 'forecast'
                      and insert_timestamp between '2021-10-19' and current_date



select sum(revenue), count(distinct rockman_id)
from revenue
where country_code = 'GR'
and timestamp between '2021-08-01 00:00:00' and '2021-08-01 23:59:59'
and rockman_id in (
select
rockman_id

from user_subscriptions
where optout > '0'
and country_code = 'GR'
and optout_reason = 'cleanup_by_errorcode'
and optout_timestamp between '2021-09-01 00:00:00' and '2021-09-01 23:59:59')

select sum(revenue), count(distinct rockman_id)
from revenue
where country_code = 'GR'
and timestamp between '2021-07-01 00:00:00' and '2021-07-01 23:59:59'
and rockman_id in (
select
rockman_id

-- SALES TARGETS
with a as (
    select country_code, max(insert_timestamp) as recent_dt
    from sales_target
    group by 1
)
         select affiliate_min_sales,
                affiliate_max_sales,
                affiliate_ecpa,
                google_min_sales,
                google_max_sales,
                google_ecpa
         from sales_target left join a on sales_target.country_code = a.country_code
         where insert_timestamp = recent_dt
           and a.country_code = 'GR'


select optout_reason, count(*) from user_subscriptions
where optout_timestamp between '2021-11-01 00:00:00' and '2021-11-01 23:59:59'
and country_code = 'GR'
group by 1





with true_val as (
select
    to_char(date_trunc('month', sale_timestamp), 'YYYY-MM-DD') as month
  , country_code
  , sum(us.home_cpa) as total_cost
  , sum(case when us.sale > 0 then 1 else null end) :: float as sales
  , sum(case when us.sale > 0 and date_diff('week', date_trunc('week', us.sale_timestamp), date_trunc('week', current_date)) >= 2  then us.tb_first_week_revenue else null end) :: float/ sales as arpu_week_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 2  then us.tb_first_month_revenue else null end) :: float/sales as arpu_month_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 3  then us.tb_first_month_revenue + us.tb_second_month_revenue else null end) :: float/sales as arpu_month_2
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 4  then us.tb_three_month_revenue else null end) :: float/sales as arpu_month_3
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 5  then us.tb_three_month_revenue + us.tb_4th_month_revenue else null end) :: float/sales as arpu_month_4
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 6  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue else null end) :: float/sales as arpu_month_5
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 7  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue else null end) :: float/sales as arpu_month_6
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 8  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue else null end) :: float/sales as arpu_month_7
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 9  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue else null end) :: float/sales as arpu_month_8
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 10 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue else null end) :: float/sales as arpu_month_9
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 11 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue else null end) :: float/sales as arpu_month_10
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 12 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue else null end) :: float/sales as arpu_month_11
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 13 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue + us.tb_12th_month_revenue else null end) :: float /sales as arpu_month_12

from user_subscriptions us
where us.sale_timestamp between '2021-01-01' and '2021-12-01'
    and us.country_code in (select distinct(country_code) from dev.icecream_all_countries)
    -- add more filters for a specific dataset

group by 1, 2
order by 1),

fcst_val as (
    select month
    , arpu_3
    , arpu_6
    , arpu_9
    , arpu_12
    , country_code
    from dev.icecream_all_countries)

select r.month::date
, r.country_code
, r.arpu_month_3 as real_arpu_3
, p.arpu_3 as forecasted_arpu_3
, r.arpu_month_6 as real_arpu_6
, p.arpu_6 as forecasted_arpu_6
, r.arpu_month_9 as real_arpu_9
, p.arpu_9 as forecasted_arpu_9
, r.arpu_month_12 as real_arpu_12
, p.arpu_12 as forecasted_arpu_12
from true_val r left join  fcst_val p on r.month = p.month and r.country_code = p.country_code




select
     system_message
     , sale
     , case when timestamp >= '2021-10-24' then 'post' else 'pre' end as oct_24_threshold
     , count(distinct rockman_id)
from user_sessions
where country_code='CH'
    and timestamp between '2021-10-14' and '2021-11-04'
group by 1, 2, 3
order by 1, 2, 3, 4 desc


with t1 as (
  --- note group by rockman_id
  -- flow events
  select
  fe.rockman_id,
  sum(case when fe.label = 'msisdn-submitted' then 1 else 0 end) as msisdn_submitted,
  sum(case when fe.label = 'msisdn-submission-failure' then 1 else 0 end) as msisdn_submission_failure,
  sum(case when fe.label = 'msisdn-submission-success' then 1 else 0 end) as msisdn_submission_success
  from flow_events fe
  where timestamp >= '2021-11-11'
  group by fe.rockman_id
)
-- views and sales over last 90 days in EE
-- Google bot views excluded

, sessions as (
  --- note rockman_id
  select
 us.rockman_id as rockman_id,
 us.country_code as country_code,
 us.affiliate_name as affiliate_name,
 us.pubid,
 us.handle_name as handle_name,
 us.operator_code as operastor_code,
 us.gateway as gateway,
  MAX(us.timestamp) as timestamp, -- note use of MAX
  sum(case when (us.impression > '0' or us.redirect > '0') then 1 else 0 end) as views,
  sum(case when us.sale > '0' then 1 else 0 end) as sales

  from user_sessions us
  where us.country_code = 'GR'
  and timestamp > '2021-11-11'
  and ip_country_code <> 'US'
  group by 1,2,3,4,5,6,7
)

select
sessions.affiliate_name,
date_part('hour', timestamp) as hour,
sum(sessions.views) as views,
sum(sessions.sales) as sales,
sum(t1.msisdn_submitted) as msisdn_submitted,
sum(t1.msisdn_submission_failure) as msisdn_submission_failure,
sum(t1.msisdn_submission_success) as msisdn_submission_success

from sessions
left join t1 on sessions.rockman_id = t1.rockman_id

group by 1,2
order by 1, 2 asc



select rockman_id
     , convert_timezone('Europe/Athens',t.timestamp)
     , date_part('hour', convert_timezone('Europe/Athens',t.timestamp)) as hour
        from transactions t
        where country_code='GR' and timestamp >= '2021-12-01' and timestamp <'2021-12-02'
        and dnstatus_detail = 'try to run it in not suitable time'
group by 3,2,1


with f as(
        select msisdn, 1 as call_2_action_fail
        from pacman
        where timestamp >= '2021-12-01'
        and country_code = 'GR'
        and event_type ='notification_trigger_mt_fail'  -- or should use gateway_api_trigger_mt_success
        group by 1),
     s as(
        select msisdn, 1 as call_2_action_success
        from pacman
        where timestamp >= '2021-12-01'
        and country_code = 'GR'
        and event_type ='notification_trigger_mt_sucess'  -- or should use gateway_api_trigger_mt_success
        group by 1),
    base as (
        select distinct msisdn
        from pacman
        where timestamp >= '2021-12-01'
        and country_code = 'GR')
select * from base left join f on base.msisdn

-- Fabio fabio
select p.timestamp, ip from pacman p
where msisdn = '41765005727'  --msisdn from SUNRISE
  and country_code = 'CH'
  and timestamp >= '2021-12-01'


-- check sale errors
with errors_distrib as (
select date_trunc('day', us.timestamp)
     , system_message
     , count(distinct rockman_id) as errors
    , sum(errors) over (partition by date_trunc('day', us.timestamp)) as total_day_sessions
    , round(errors::float *100 / total_day_sessions, 1) as pct_of_daily_sessions
from user_sessions us
where country_code ='CH'
  and timestamp between '2021-12-28 00:00' and '2022-01-09 23:59'
and sale = 0
and system_message is not null
group by 1,2
order by 1, 3 desc)
select * from errors_distrib
where pct_of_daily_sessions >= 1  --Filtering


select --date_trunc('day', us.timestamp)
      system_message
     , count(distinct rockman_id) as errors
     , count(distinct rockman_id)/sum(count(distinct rockman_id)) as pct
    --, sum(errors) over (partition by date_trunc('day', us.timestamp)) as total_day_sessions
    --, round(errors::float *100 / total_day_sessions, 1) as pct_of_daily_sessions
from user_sessions us
where country_code ='CH'
  and timestamp between '2021-12-28 00:00' and '2022-01-09 23:59'
  and date(timestamp) != '2022-01-02'
  and date(timestamp) != '2022-01-09'
and sale = 0
and system_message is not null
group by 1
order by 2 desc




with users_base as (
    select msisdn from user_subscriptions
    where sale_timestamp >= '2021-12-14'
    and service_identifier1 !='frogstargames'
    and country_code = 'GR'
),
errors as (
    select
           msisdn
         , case when event_type ilike '%fail' then 'fail' else 'success' end notification
         , split_part(system_message, '---', 1) as operator
         , split_part(system_message, '---', 2) as error
    from pacman
    where country_code = 'GR'
      and timestamp>='2021-12-14'
      and event_type ilike 'notification%'
        and system_message ilike 'GR%')
select
    notification, count(*), count(*)::float * 100 / sum(count(*)) over () as pct
from users_base left join errors using (msisdn)
group by 1


select
    case when us.google_campaign_id in ('13051430594') then 'variant' else us.google_campaign_id end as group,
       sum(us.impression) as impressions,
       sum(us.sale) as sales,
       (sum(us.sale)::float / sum(us.impression)::float * 100) as cr,
       sum(usu.tb_three_month_revenue) as revenue,
       sum(usu.tb_first_week_revenue) / sum(us.sale) as arpu_7,
       sum(usu.tb_second_week_revenue + usu.tb_first_week_revenue) / sum(us.sale) as arpu_14,
       sum(usu.tb_first_month_revenue) / sum(us.sale) as arpu_30,
       sum(usu.tb_three_month_revenue) / sum(us.sale) as arpu_to_date

from user_sessions us left join user_subscriptions usu using(rockman_id)
where us.timestamp >= '2021-12-01'  -- change this
and us.country_code = 'CH'
and us.google_campaign_id is not null
group by 1

select tb_first_week_revenue,
       tb_second_week_revenue
from user_subscriptions us
where us.timestamp >= '2021-12-01'  -- change this
and us.country_code = 'CH'
and us.google_campaign_id is not null

select service_identifier1
from user_subscriptions
where country_code='GR' and timestamp >='2021-12-30'
group by 1

winimi


with users_base as (
    select msisdn from user_subscriptions
    where sale_timestamp >= '2021-12-30'
    and service_identifier1 !='winimi' -- -'frogstargames'
    and country_code = 'GR'
),
errors as (
    select
           msisdn
         , case when event_type ilike '%fail' then 'fail' else 'success' end notification
         , split_part(system_message, '---', 1) as operator
         , split_part(system_message, '---', 2) as error
    from pacman
    where country_code = 'GR'
      and timestamp>='2021-12-30'
      and event_type ilike 'notification%'
        and system_message ilike 'GR%')
select
    notification, count(*), count(*)::float * 100 / sum(count(*)) over () as pct
from users_base left join errors using (msisdn)
group by 1

--bef fail 26%
--after success 72%

SELECT country_code, max(execution_timestamp)
from icecream_forecast
group by 1
order by 1

with true_val as (
select
    to_char(date_trunc('month', sale_timestamp), 'YYYY-MM-DD') as month
  , country_code
  , sum(us.home_cpa) as total_cost
  , sum(case when us.sale > 0 then 1 else null end) :: float as sales
  , sum(case when us.sale > 0 and date_diff('week', date_trunc('week', us.sale_timestamp), date_trunc('week', current_date)) >= 2  then us.tb_first_week_revenue else null end) :: float/ sales as arpu_week_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 2  then us.tb_first_month_revenue else null end) :: float/sales as arpu_month_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 3  then us.tb_first_month_revenue + us.tb_second_month_revenue else null end) :: float/sales as arpu_month_2
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 4  then us.tb_three_month_revenue else null end) :: float/sales as arpu_month_3
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 5  then us.tb_three_month_revenue + us.tb_4th_month_revenue else null end) :: float/sales as arpu_month_4
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 6  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue else null end) :: float/sales as arpu_month_5
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 7  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue else null end) :: float/sales as arpu_month_6
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 8  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue else null end) :: float/sales as arpu_month_7
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 9  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue else null end) :: float/sales as arpu_month_8
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 10 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue else null end) :: float/sales as arpu_month_9
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 11 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue else null end) :: float/sales as arpu_month_10
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 12 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue else null end) :: float/sales as arpu_month_11
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 13 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue + us.tb_12th_month_revenue else null end) :: float /sales as arpu_month_12

from user_subscriptions us
where us.sale_timestamp between '2021-01-01' and '2022-01-01'
    and us.country_code in (select distinct(country_code) from icecream_forecast)
    -- add more filters for a specific dataset

group by 1, 2
order by 1),

fcst_val as (
    select month
    , arpu_3
    , arpu_6
    , arpu_9
    , arpu_12
    , country_code
    from icecream_forecast --dev.icecream_all_countries
    )

select p.month::date
, p.country_code
, r.arpu_month_3 as real_arpu_3
, p.arpu_3 as forecasted_arpu_3
, r.arpu_month_6 as real_arpu_6
, p.arpu_6 as forecasted_arpu_6
, r.arpu_month_9 as real_arpu_9
, p.arpu_9 as forecasted_arpu_9
, r.arpu_month_12 as real_arpu_12
, p.arpu_12 as forecasted_arpu_12
from true_val r right join  fcst_val p on r.month = p.month and r.country_code = p.country_code


select distinct country_code from icecream_forecast


with sessions as (
select
       case when google_campaign_id = 14979121828 then 'variant' else 'control' end as gr,
       sale,
       rockman_id
from user_sessions
where operator_code = 'GR_COSMOTE'
  and country_code = 'GR'
  and affiliate_name like '%DMB%'
  and timestamp >= '2021-11-01'
    and timestamp <= current_date - interval '2 weeks'),

rev as (
select rockman_id
     , sum(tb_first_week_revenue + tb_second_week_revenue) as two_week_revenue
from user_subscriptions
where operator_code = 'GR_COSMOTE'
  and country_code = 'GR'
  and affiliate_name like '%DMB%'
  and timestamp >= '2021-11-01'
    and timestamp <= current_date - interval '2 weeks'
group by 1)



--icecream
with true_val as (
select
    to_char(date_trunc('month', sale_timestamp), 'YYYY-MM-DD') as month
  , country_code
  , sum(us.home_cpa) as total_cost
  , sum(case when us.sale > 0 then 1 else null end) :: float as sales
  , sum(case when us.sale > 0 and date_diff('week', date_trunc('week', us.sale_timestamp), date_trunc('week', current_date)) >= 2  then us.tb_first_week_revenue else null end) :: float/ sales as arpu_week_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 2  then us.tb_first_month_revenue else null end) :: float/sales as arpu_month_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 3  then us.tb_first_month_revenue + us.tb_second_month_revenue else null end) :: float/sales as arpu_month_2
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 4  then us.tb_three_month_revenue else null end) :: float/sales as arpu_month_3
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 5  then us.tb_three_month_revenue + us.tb_4th_month_revenue else null end) :: float/sales as arpu_month_4
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 6  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue else null end) :: float/sales as arpu_month_5
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 7  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue else null end) :: float/sales as arpu_month_6
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 8  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue else null end) :: float/sales as arpu_month_7
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 9  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue else null end) :: float/sales as arpu_month_8
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 10 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue else null end) :: float/sales as arpu_month_9
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 11 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue else null end) :: float/sales as arpu_month_10
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 12 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue else null end) :: float/sales as arpu_month_11
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 13 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue + us.tb_12th_month_revenue else null end) :: float /sales as arpu_month_12

from user_subscriptions us
where us.sale_timestamp between '2021-01-01' and '2022-01-01'
    and us.country_code in (select distinct(country_code) from icecream_forecast)
    -- add more filters for a specific dataset

group by 1, 2
order by 1),
;

with a as(
select
 us.country_code
  , to_char(date_trunc('month', sale_timestamp), 'YYYY-MM-DD') as month
  , sum(us.home_cpa) as total_cost
  , sum(case when us.sale > 0 then 1 else null end) as sales

  , sum(case when us.sale > 0 and date_diff('week', date_trunc('week', us.sale_timestamp), date_trunc('week', current_date)) >= 2  then us.tb_first_week_revenue else null end) :: float as revenue_week_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 2  then us.tb_first_month_revenue else null end) :: float as revenue_month_1
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 3  then us.tb_first_month_revenue + us.tb_second_month_revenue else null end) :: float as revenue_month_2
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 4  then us.tb_three_month_revenue else null end) :: float as revenue_month_3
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 5  then us.tb_three_month_revenue + us.tb_4th_month_revenue else null end) :: float as revenue_month_4
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 6  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue else null end) :: float as revenue_month_5
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 7  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue else null end) :: float as revenue_month_6
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 8  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue else null end) :: float as revenue_month_7
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 9  then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue else null end) :: float as revenue_month_8
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 10 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue else null end) :: float as revenue_month_9
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 11 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue else null end) :: float as revenue_month_10
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 12 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue else null end) :: float as revenue_month_11
  , sum(case when us.sale > 0 and date_diff('month', date_trunc('month', us.sale_timestamp), date_trunc('month', current_date)) >= 13 then us.tb_three_month_revenue + us.tb_4th_month_revenue + us.tb_5th_month_revenue + us.tb_6th_month_revenue + us.tb_7th_month_revenue + us.tb_8th_month_revenue + us.tb_9th_month_revenue + us.tb_10th_month_revenue + us.tb_11th_month_revenue + us.tb_12th_month_revenue else null end) :: float as revenue_month_12


from user_subscriptions us
where us.sale_timestamp between '2021-01-19' and '2022-01-19'
and country_code = 'GR'
    -- add more filters for a specific dataset

group by 1, 2
order by 1, 2)
select avg(revenue_week_1) from a where month < '2021-12-01'

select execution_timestamp, arpu_12
from icecream_forecast where execution_timestamp > '2022-01-14'
and arpu_12 is not null

select max(execution_timestamp) from icecream_forecast

SELECT sti.schema, sti.table, sq.endtime, sq.querytxt
FROM
    (SELECT MAX(query) as query, tbl, MAX(i.endtime) as last_insert
    FROM stl_insert i
    GROUP BY tbl
    ORDER BY tbl) inserts
JOIN stl_query sq ON sq.query = inserts.query
JOIN svv_table_info sti ON sti.table_id = inserts.tbl
ORDER BY inserts.last_insert DESC;

-- CHECK queries QUERIES
select * from stl_query order by starttime desc limit 20
select * from  svv_table_info limit 10

SELECT country_code, max(execution_timestamp)
FROM icecream_error_margin
group by 1