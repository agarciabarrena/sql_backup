select type from message_history where create_date_time > '2020-01-01' group by 1

select
distinct (msisdn)
from mo
where
      created_at >= '2020-09-29'
  and lower(mo_text) like 'stop'
and operator_id = 379  -- GR_VODAFONE


select * from gateway where name like 'echo%'   gateway_id = 34 #echovox

select count(*)
from transaction
where gateway_id = 34 and created_at between '2021-10-01' and '2021-10-31'
and status='succeed'

# NON OPTIMIZED
select operador, count(transactions)
from (select operator_fqn as operador
                    #, month(created_at)         as month
                    , sam_transaction_id as transactions
               from VIEW_TRANSACTION
               where country = 'Greece'
                 and status != 'failed'
                 and gateway = 'Paydash'
                 and date(created_at) between '2020-02-28' and '2020-02-29'#'2020-03-31'
 limit 10000   ) as a
group by 1

# OPTIMIZED
select o.name as operator,
       month(t.created_at) as month,
       IFNULL(p.product_identifier_1, srv.name) service_identifier1,
       pd.product_identifier_2 service_identifier2,
       pd.product_identifier_3 service_identifier3,
       count(t.id) as nr_transactions,
       sum(b.fee) as local_curr_revenue
from transaction partition (jul2020) t
join billing partition (jul2020) b on t.billing_id = b.id
join subscription s on b.subscription_id = s.id
join operator o on s.operator_id = o.id
join gateway g on t.gateway_id = g.id
join country c on o.country_id = c.id
JOIN mcb.scenario_service ss ON s.scenario_service_id = ss.id
JOIN mcb.service srv ON ss.service_id = srv.id
JOIN product_distribution pd ON pd.id = s.product_distribution_id
JOIN product p ON pd.product_id = p.id
where c.name = 'Greece'
  and t.status = 'succeed'
  and g.name = 'Paydash'
  and date(t.created_at) >= '2020-07-01'
  and date(t.created_at) < '2020-08-01'
group by 1, 2, 3, 4, 5


select * from subscription
where start_date_time > '2020-11-17'
and gateway_id = 99


select date(created_at), date(updated_at), mo_text
from mo where gateway_id = 67
          and created_at > '2020-11-26'



select * from gateways_operators where code like 'ES_%'  # ES_ORANGE = 20


select * from subscription where operator_id = 20 and start_date_time >= current_date


select  from user_session where country

SELECT * FROM pin
JOIN pin_history ON pin.id = pin_history.pin_id
WHERE msisdn = '381641287857';

select * from gateway  gateway id 34 echovox

select * from subscription s inner join transaction t on s.id = t.subscription_id
where s.gateway_id = 34  and t.gateway_id = 34 and t.created_at between '2021-10-01' and '2021-10-31'
  and s.operator_id in (695, 696,697)
  and t.status='succeed'

