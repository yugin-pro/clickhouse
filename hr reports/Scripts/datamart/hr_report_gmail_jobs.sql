--hr_report 


create or replace table datamart.hr_report_gmail_jobs ENGINE = ReplacingMergeTree ORDER by (id,date) as
select 
distinct id
, date
, 'company' organisation
, 'vacansy' job_description
, 0 is_application_confirmation
, 0 is_application_declined
, 0 is_interview_invitation
, 0 comunication_number
from analyt_jobmails.message_parsed
left join analyt_jobmails.message_dates using(id) 

