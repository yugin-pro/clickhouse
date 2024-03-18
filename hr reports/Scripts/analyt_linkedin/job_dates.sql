--job_dates


insert into table analyt_linkedin.job_dates 
select 
 cast(extract(jobPostingUrn,'(\d+)') as UInt32) id
, toDateTimeOrNull( substr(timeAt, 1, length(timeAt) - 3)) created_ts
, toDateTimeOrNull( substr(request_ts, 1, length(timeAt) - 3)) recorded_ts
, toDate(created_ts) created_date 
, toHour(created_ts) created_hours
, toMinute(created_ts) created_minutes
from extracted.linkedin_jobs
where id is not null and created_ts is not null and recorded_ts is not null


--schema
CREATE TABLE analyt_linkedin.job_dates
(

    `id` UInt32,

    `created_ts` DateTime,

    `recorded_ts` DateTime,

    `created_date` Date,

    `created_hours` UInt8,

    `created_minutes` UInt8
)
ENGINE = ReplacingMergeTree
ORDER BY (id,
 recorded_ts)
SETTINGS index_granularity = 8192;