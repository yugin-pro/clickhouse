--hr_report_linkedin_jobs


create or replace table datamart.hr_report_linkedin_jobs ENGINE = MergeTree ORDER by id as
select id,lower(category) category,created_date
,	is_apropriate
	from (
select 
	id,category
	,created_date
from analyt_linkedin.job_category
left join analyt_linkedin.job_dates final using(id) 
)
left join analyt_linkedin.job_extra final using(id)
settings joined_subquery_requires_alias=0