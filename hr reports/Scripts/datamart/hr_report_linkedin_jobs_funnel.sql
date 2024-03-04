--hr_report_linkedin_jobs_funnel


create or replace table datamart.hr_report_linkedin_jobs_funnel ENGINE = MergeTree ORDER by id as
select a.*, b.v_name from (
select * from (
select * from analyt_linkedin.job_parsed final
left join (select id , min(created_date) created_date from analyt_linkedin.job_dates final group by id) using(id)
)
left join analyt_linkedin.job_extra final using(id)
) a
left join (
	select '' v_name,'' o_name, * from analyt_jobmails.message_vacancy
	left join analyt_jobmails.message_organisation using(id)
	where id in (select id from analyt_jobmails.message_new_applications final
	where is_new = 1)	
) b on company = o_name and jobPostingTitle = v_name
settings joined_subquery_requires_alias=0