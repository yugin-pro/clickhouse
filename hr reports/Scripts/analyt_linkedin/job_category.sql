--job_category


create or replace table analyt_linkedin.job_category ENGINE = MergeTree() ORDER by id as
select distinct
cast(extract(jobPostingUrn,'(\d+)') as UInt32) id
,category
from extracted.linkedin_jobs



--schema
CREATE TABLE analyt_linkedin.job_category
(

    `id` UInt32,

    `category` String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;
