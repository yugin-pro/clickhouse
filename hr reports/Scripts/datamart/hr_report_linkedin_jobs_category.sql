--hr_report_linkedin_jobs


create or replace table datamart.hr_report_linkedin_jobs_category ENGINE = MergeTree ORDER by id as
select id
,toDate(created_ts) created,toDate(request_ts) last_requested,category_group category, is_apropriate,recreated_index,recreated_amount,remote
 ,floor(dateDiff('day', created, last_requested) / 30,0) keep_updated_month
from (
select id
,created_ts,request_ts,category_group ,recreated_index,recreated_amount,remote
,is_apropriate
from analyt_linkedin.job_parsed final
left join analyt_linkedin.job_extra using(id)
)
array join category_group