--job_parsed

create or replace table analyt_linkedin.job_parsed ENGINE = ReplacingMergeTree(request_ts)
ORDER BY id as
--insert into table analyt_linkedin.job_parsed
select 
cast(extract(jobPostingUrn,'(\d+)') as UInt32) id
, if(length(timeAt) == 13,toString(timeAt),'1682422731000') time_at
, toDateTime(substr(time_at, 1, length(time_at) - 3)) created_ts
, toDateTime(substr(request_ts, 1, length(request_ts) - 3)) request_ts
,trim(jobPostingTitle) title
,trim(company) company
,lower(arrayStringConcat(extractAll(company,'\w'),'')) company_related_name 
,trim(actionTarget) company_link
,concat('https://www.linkedin.com/jobs/view/',toString(id)) job_url
,trim(extract(text_1,'(.*)applica')) applicants
,match(text_1,'Easy Apply') or match(text_2,'Easy Apply') is_easy_apply
,trim(splitByChar('(',region)[1]) geo
,replaceOne(trim(splitByChar('(',region)[2]),')','') remote
, groupUniqArray(category) over (partition by jobPostingUrn) category_group
, dense_rank(id) over (partition by company, title order by id asc ) recreated_index
, count(distinct id) over (partition by company, title ) recreated_amount
, count(distinct title) over (partition by company ) different_job_per_company
from extracted.linkedin_jobs final
