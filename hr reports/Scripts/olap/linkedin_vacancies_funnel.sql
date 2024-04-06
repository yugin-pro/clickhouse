--company funnel
create or replace table olap.linkedin_vacancies_funnel engine=MergeTree order by company as 
select 
	company ,created_date
	, sum(vacansies) vacansies 
	, sum(apropriate) apropriate
	, max(has_data) has_data
	, max(has_web) has_web
	, max(has_cro) has_cro
	, sum(application) application 
from (
select
company
, created_date
, count(distinct id) vacansies
,countIf(distinct company,is_apropriate = 1) apropriate
,max(match(lower(jobPostingTitle),'data')) has_data
,max(match(lower(jobPostingTitle),'web')) has_web
,max(match(lower(jobPostingTitle),'cro')) has_cro
--,countIf(distinct company,match(lower(jobPostingTitle),'web')) web_analyst
--,countIf(distinct company,match(lower(jobPostingTitle),'cro')) cro_analyst
,0 application
from datamart.hr_report_linkedin_jobs_funnel
where created_date > '2024-01-01' and notEmpty(company) 
group by 1,2

union all

select
 organisation,`date`,0 v,0,0,0,0, count(id) application
from datamart.hr_report_gmail_jobs
where is_new = 1 and notEmpty(organisation) and organisation not like '%XXX%' 
group by 1,2
)
group by 1,2