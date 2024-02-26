--job_dates


create or replace table analyt_linkedin.job_dates ENGINE = ReplacingMergeTree ORDER by (id, trackingId) as
select 
trackingId
, cast(extract(jobPostingUrn,'(\d+)') as UInt32) id
, toDateTimeOrNull( substr(timeAt, 1, length(timeAt) - 3)) created_ts
, toDateTimeOrNull( substr(request_ts, 1, length(timeAt) - 3)) recorded_ts
, toDate(created_ts) created_date 
, toHour(created_ts) created_hours
, toMinute(created_ts) created_minutes 
from extracted.linkedin_jobs
