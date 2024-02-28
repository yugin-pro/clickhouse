--hr_report 


create or replace table datamart.hr_report_gmail_jobs ENGINE = ReplacingMergeTree ORDER by (id,date) as
select id
, date, 'company' organisation
, 'vacansy' job_description
, is_new is_application_confirmation
, is_declined is_application_declined
, is_invitation is_interview_invitation
, 0 comunication_number 
from (
select * from (
select * from (
select * from (
select * from (
select *
from (
	select 
	distinct id, date
		from analyt_jobmails.message_parsed 
	left join analyt_jobmails.message_dates using(id)
)
left join analyt_jobmails.message_new_applications  using(id) 
)
left join analyt_jobmails.message_declined using(id) 
)
left join analyt_jobmails.message_invites  using(id) 
)
left join analyt_jobmails.message_organisation using(id) 
)
left join analyt_jobmails.message_vacancy using(id) 
)
settings joined_subquery_requires_alias=0