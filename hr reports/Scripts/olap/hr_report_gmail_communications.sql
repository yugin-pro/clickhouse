--gmail_communications
create or replace table olap.hr_report_gmail_communications engine=MergeTree order by date as 
select `date`
, organisation
, count(distinct thread_id) topics
, count(distinct id) incom_messages
, sum(is_service) service_messages
, sum(is_new) new_messages
, sum(is_viewed) viewed_messages
, sum(is_declined) declined_messages
, sum(is_interview) interview_messages
from datamart.hr_report_gmail_jobs final
where is_replay = 0
group by 1,2