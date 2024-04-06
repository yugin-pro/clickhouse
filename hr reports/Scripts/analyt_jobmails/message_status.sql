--message_status


create or replace table analyt_jobmails.message_status ENGINE = MergeTree ORDER by id as
select 
id,thread_id
--body,subject,message_from
,case
	when subject like '%was sent to%' then 1
	when subject like '%for applying %' then 1
	when subject like '%have received%' then 1
	when lower(subject) like '%we received%' then 1
	when lower(subject) like '%application received%' then 1
	when lower(subject) like '%voor je sollicitatie%' then 1
	when lower(body) like '%we received%' then 1
	else 0
end as is_new
, case 
	when lower(subject) like '%update%' then 1
	when lower(body) like '%update%' then 1
	when lower(subject) like '%viewed%' then 1
	when lower(body) like '%consid%' then 1
	else 0
end as is_viewed
, case 
	when lower(subject) like '%appriciate%' then 1
	when lower(body) like '%not contini%' then 1
	when subject like '%viewed%' then 1
	else 0
end as is_declined
, case 
	when lower(subject) like '%interview%' then 1
	when lower(body) like '%interview%' then 1
	else 0
end as is_interview
from analyt_jobmails.message_parsed
where 
is_replay = 0 and is_service = 1
--having status not like 'new'
--order by subject

union all

select 
-- * 
id,thread_id
, case 
	when interaction_mumber = 1 then 1
	else 0
end as is_new
, case 
	when lower(subject) like '%update%' then 1
	when lower(body) like '%update%' then 1
	when lower(subject) like '%viewed%' then 1
	when lower(body) like '%consid%' then 1
	else 0
end as is_viewed
, case 
	when lower(subject) like '%appriciate%' then 1
	when lower(body) like '%not contini%' then 1
	when subject like '%viewed%' then 1
	else 0
end as is_declined
, case 
	when lower(subject) like '%interview%' then 1
	when lower(body) like '%interview%' then 1
	else 0
end as is_interview
from (
select 
--*
id,thread_id,subject,body
, row_number() over (partition by message_from_email order by ts asc) interaction_mumber
from analyt_jobmails.message_parsed
where 
is_replay = 0 and is_service = 0
)