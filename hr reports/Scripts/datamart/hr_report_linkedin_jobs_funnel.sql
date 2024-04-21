--hr_report_linkedin_jobs_funnel


create or replace table datamart.hr_report_linkedin_jobs_funnel ENGINE = MergeTree ORDER by id as
select 
a.id,created_ts,title,company,company_related_name,company_link,job_url,applicants,is_easy_apply,remote,geo
,toInt16OrZero(extract(applicants,'\d*')) applicant_number
,length(category_group) category_number
,recreated_index,recreated_amount,different_job_per_company,is_apropriate
, b.id is_company_interaction from (
select *
from analyt_linkedin.job_parsed a final 
left join analyt_linkedin.job_extra using(id)
) a
left join analyt_jobmails.message_organisation b on company_related_name=related_name 
