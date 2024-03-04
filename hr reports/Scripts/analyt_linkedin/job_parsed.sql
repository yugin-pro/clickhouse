--job_parsed

insert into table analyt_linkedin.job_parsed
select 
cast(extract(jobPostingUrn,'(\d+)') as UInt32) id
,jobPostingTitle,title,region,company,actionTarget
,toInt64(request_ts) request_ts
from extracted.linkedin_jobs
