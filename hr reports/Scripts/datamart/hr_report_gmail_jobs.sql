--hr_report 


create or replace table datamart.hr_report_gmail_jobs ENGINE = ReplacingMergeTree ORDER by (id,date) as
select *
, row_number() over (partition by organisation order by date asc) comunication_number 
from (
select id,thread_id, date,is_replay,is_service,is_new,is_viewed,is_declined,is_interview,organisation
from (
	select 
	distinct id,thread_id, date,is_replay,is_service,is_new,is_viewed,is_declined,is_interview
		from analyt_jobmails.message_parsed 
	left join analyt_jobmails.message_status using(id)
)
left join analyt_jobmails.message_organisation using(id) 
)
settings joined_subquery_requires_alias=0
