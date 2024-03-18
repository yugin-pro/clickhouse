--job_parsed

insert into table analyt_linkedin.job_parsed
select 
cast(extract(jobPostingUrn,'(\d+)') as UInt32) id
,jobPostingTitle,title,region,company,actionTarget
,toInt64(request_ts) request_ts
from extracted.linkedin_jobs



--schema
CREATE TABLE analyt_linkedin.job_parsed
(

    `id` UInt32,

    `jobPostingTitle` String,

    `title` String,

    `region` String,

    `company` String,

    `actionTarget` String,

    `request_ts` Int64
)
ENGINE = ReplacingMergeTree(request_ts)
ORDER BY id
SETTINGS index_granularity = 8192;