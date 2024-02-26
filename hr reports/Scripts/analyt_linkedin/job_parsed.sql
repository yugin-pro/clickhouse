--job_dates


create or replace table analyt_linkedin.job_parsed ENGINE = ReplacingMergeTree(request_ts) ORDER by (id) as
select 
cast(extract(jobPostingUrn,'(\d+)') as UInt32) id
,jobPostingTitle,title,region,company,actionTarget
,toInt64(request_ts) request_ts
from extracted.linkedin_jobs
