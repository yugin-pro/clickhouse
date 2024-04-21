--hr_report_category_jobs

create or replace table olap.hr_report_category_jobs engine=MergeTree order by created as 
select
created, trim(lower(category)) search_keywords,remote 
,count(distinct id) jobs
,countIf(distinct id,is_apropriate) apropriate_jobs
,sum(keep_updated_month) waiting_months
,sum(recreated_amount) recreated_amount
from datamart.hr_report_linkedin_jobs_category 
GROUP BY GROUPING SETS
(
    (created, search_keywords, remote),
    (created,remote)
)
