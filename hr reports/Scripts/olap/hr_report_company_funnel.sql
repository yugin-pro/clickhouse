--company funnel
create or replace table olap.hr_report_company_funnel engine=MergeTree order by company as 
select
company,remote,geo
, toDate(created_ts) created_date
, count(distinct id) vacansies
,sum(applicant_number) aplicants
,countIf(distinct company,is_apropriate = 1) apropriate
,max(match(lower(title),'data')) has_data
,max(match(lower(title),'web')) has_web
,max(match(lower(title),'cro')) has_cro
--,countIf(distinct company,match(lower(jobPostingTitle),'web')) web_analyst
--,countIf(distinct company,match(lower(jobPostingTitle),'cro')) cro_analyst
, countIf(distinct company,length(is_company_interaction)>0) application
from datamart.hr_report_linkedin_jobs_funnel
where notEmpty(company) 
group by 1,2,3,4
