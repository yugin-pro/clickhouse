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
